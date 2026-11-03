// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

package lansync

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/miekg/dns"
)

func TestDiscoveryAddressQueryUDP(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	server := newTestManager(t, key, "main", nil)
	client := newTestManager(t, key, "main", nil)
	info := server.DiscoveryInfo()
	entry := &mdns.ServiceEntry{Name: info.Instance, Port: info.Port, InfoFields: []string{"v=" + info.TXT["v"], "f=" + info.TXT["f"], "scope=" + info.TXT["scope"]}}
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.queryDiscoveryAddresses(ctx, conn, listener.LocalAddr().(*net.UDPAddr), map[string][]*mdns.ServiceEntry{"phone.": {entry}})
	}()
	listener.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 4096)
	n, from, err := listener.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	query := new(dns.Msg)
	if err = query.Unpack(buf[:n]); err != nil {
		t.Fatal(err)
	}
	if len(query.Question) != 1 || query.Question[0].Name != "phone." || query.Question[0].Qtype != dns.TypeA || query.Question[0].Qclass != dns.ClassINET|1<<15 {
		t.Fatalf("unexpected query: %v", query.Question)
	}
	response := new(dns.Msg)
	response.SetReply(query)
	response.Answer = []dns.RR{&dns.A{Hdr: dns.RR_Header{Name: "phone.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120}, A: net.ParseIP("127.0.0.3")}}
	for _, invalid := range []struct{ opcode, rcode int }{{dns.OpcodeQuery, dns.RcodeNameError}, {dns.OpcodeNotify, dns.RcodeSuccess}} {
		response.Opcode, response.Rcode = invalid.opcode, invalid.rcode
		data, packErr := response.Pack()
		if packErr != nil {
			t.Fatal(packErr)
		}
		if _, err = listener.WriteToUDP(data, from); err != nil {
			t.Fatal(err)
		}
	}
	response.Opcode, response.Rcode = dns.OpcodeQuery, dns.RcodeSuccess
	response.Answer = nil
	for _, address := range []string{"127.0.0.2", "127.0.0.1"} {
		response.Answer = append(response.Answer, &dns.A{Hdr: dns.RR_Header{Name: "phone.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120}, A: net.ParseIP(address)})
	}
	data, err := response.Pack()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = listener.WriteToUDP(data, from); err != nil {
		t.Fatal(err)
	}
	waitForPeerCount(t, client, 1)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("query did not stop on cancellation")
	}
	client.peerMu.RLock()
	defer client.peerMu.RUnlock()
	if len(client.peers) != 2 {
		t.Fatalf("accepted malformed DNS response: %d peers", len(client.peers))
	}
}

func TestDiscoveryAddressesPreserveReachableCandidate(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	server := newTestManager(t, key, "main", nil)
	client := newTestManager(t, key, "main", nil)
	info := server.DiscoveryInfo()
	entry := &mdns.ServiceEntry{Name: info.Instance, Host: "phone.local.", Port: info.Port}
	for name, value := range info.TXT {
		entry.InfoFields = append(entry.InfoFields, name+"="+value)
	}
	hosts := map[string][]*mdns.ServiceEntry{"phone.local.": {entry}}
	response := &dns.Msg{MsgHdr: dns.MsgHdr{Response: true}}
	for _, address := range []string{"127.0.0.1", "127.0.0.2"} {
		response.Answer = append(response.Answer, &dns.A{
			Hdr: dns.RR_Header{Name: "PHONE.local.", Rrtype: dns.TypeA, Class: dns.ClassINET | 1<<15, Ttl: 120},
			A:   net.ParseIP(address),
		})
	}
	data, err := response.Pack()
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(dns.Msg)
	if err = decoded.Unpack(data); err != nil {
		t.Fatal(err)
	}
	client.addDiscoveryAddresses(decoded, hosts, map[string]bool{})
	waitForPeerCount(t, client, 1)
	client.peerMu.RLock()
	if len(client.peers) != 2 {
		t.Fatalf("lost an address candidate: %d", len(client.peers))
	}
	client.peerMu.RUnlock()
	// 不可达候选重复出现时，可达地址的认证会话仍然有效。
	decoded.Answer = decoded.Answer[1:]
	client.addDiscoveryAddresses(decoded, hosts, map[string]bool{})
	if client.ConnectedPeerCount() != 1 {
		t.Fatal("unreachable candidate replaced the authenticated peer")
	}
}

func TestDiscoveryAddressesRejectUnrelatedRecords(t *testing.T) {
	client := newTestManager(t, []byte("0123456789abcdef0123456789abcdef"), "main", nil)
	info := client.DiscoveryInfo()
	entry := &mdns.ServiceEntry{Name: "another-device", Port: info.Port, InfoFields: []string{"v=" + info.TXT["v"], "f=" + info.TXT["f"], "scope=" + info.TXT["scope"]}}
	hosts := map[string][]*mdns.ServiceEntry{"phone.local.": {entry}}
	response := &dns.Msg{}
	for _, tc := range []struct {
		host, ip string
		ttl      uint32
	}{
		{"other.local.", "127.0.0.1", 120},
		{"phone.local.", "127.0.0.1", 0},
		{"phone.local.", "203.0.113.1", 120},
	} {
		response.Answer = append(response.Answer, &dns.A{Hdr: dns.RR_Header{Name: tc.host, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: tc.ttl}, A: net.ParseIP(tc.ip)})
	}
	client.addDiscoveryAddresses(response, hosts, map[string]bool{})
	if client.DiscoveredPeerCount() != 0 {
		t.Fatal("accepted unrelated, expired or public address")
	}
	for _, host := range []string{"", ".", "phone"} {
		if validDiscoveryHost(host) {
			t.Fatalf("accepted hostname %q", host)
		}
	}
	service, err := mdns.NewMDNSService("peer", ServiceName, "", "", 12345, []net.IP{net.ParseIP("192.168.1.2")}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !validDiscoveryHost(service.HostName) {
		t.Fatalf("rejected default advertised hostname: %s", service.HostName)
	}
}

func TestDiscoveryTargetsRejectUnavailableInterfaces(t *testing.T) {
	ip := net.ParseIP("192.168.122.1")
	ready := net.FlagUp | net.FlagRunning | net.FlagMulticast
	for _, flags := range []net.Flags{ready &^ net.FlagRunning, ready &^ net.FlagMulticast, ready | net.FlagPointToPoint, ready | net.FlagLoopback, 0} {
		targets := groupDiscoveryTargets([]net.IP{ip}, []networkInterface{{iface: net.Interface{Flags: flags}, addrs: []net.Addr{&net.IPNet{IP: ip}}}})
		if len(targets) != 0 {
			t.Fatalf("invalid interface escaped via fallback: %v", flags)
		}
	}
	if targets := groupDiscoveryTargets([]net.IP{ip}, nil); len(targets) != 1 {
		t.Fatal("native address fallback missing when interface enumeration is unavailable")
	}
	partial := []networkInterface{
		{iface: net.Interface{Flags: net.FlagUp | net.FlagLoopback}, addrs: []net.Addr{&net.IPNet{IP: net.ParseIP("127.0.0.1")}}},
		{iface: net.Interface{Flags: ready}, unavailable: true},
	}
	if targets := groupDiscoveryTargets([]net.IP{ip}, partial); len(targets) != 1 || targets[0].iface != nil {
		t.Fatal("native address lost after partial interface enumeration")
	}
}

func TestDiscoveryAddressCandidateLimit(t *testing.T) {
	client := newTestManager(t, []byte("0123456789abcdef0123456789abcdef"), "main", nil)
	entry := &mdns.ServiceEntry{Name: "other", Port: 12345}
	hosts := map[string][]*mdns.ServiceEntry{"phone.": {entry}}
	response := new(dns.Msg)
	for i := 1; i <= 100; i++ {
		record := &dns.A{Hdr: dns.RR_Header{Name: "phone.", Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 120}, A: net.IPv4(192, 168, 1, byte(i))}
		response.Answer = append(response.Answer, record, record)
	}
	seen := map[string]bool{}
	client.addDiscoveryAddresses(response, hosts, seen)
	client.addDiscoveryAddresses(response, hosts, seen)
	if len(seen) != 64 {
		t.Fatalf("candidate budget not enforced: %d", len(seen))
	}
	if client.DiscoveredPeerCount() != 0 {
		t.Fatal("invalid scope bypassed authentication prefilter")
	}
}
