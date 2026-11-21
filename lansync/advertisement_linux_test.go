// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !android

package lansync

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/miekg/dns"
)

func TestLinuxMDNSReceiverChecksIncomingInterface(t *testing.T) {
	loopback, err := net.InterfaceByName("lo")
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range []struct {
		network string
		ip      string
	}{{"udp4", "127.0.0.1"}, {"udp6", "::1"}} {
		t.Run(family.network, func(t *testing.T) {
			serviceIP := net.ParseIP("192.168.1.2")
			service, err := mdns.NewMDNSService("peer", ServiceName, "", "peer.local.", 12345, []net.IP{serviceIP}, []string{"v=1"})
			if err != nil {
				t.Fatal(err)
			}
			for _, testCase := range []struct {
				name           string
				interfaceIndex int
				wantResponse   bool
			}{
				{"other interface", loopback.Index + 1, false},
				{"receiving interface", loopback.Index, true},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					listener, err := net.ListenUDP(family.network, &net.UDPAddr{IP: net.ParseIP(family.ip)})
					if err != nil {
						if family.network == "udp6" {
							t.Skipf("IPv6 loopback unavailable: %v", err)
						}
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = listener.Close() })
					if err = startLinuxMDNSReceiver(listener, family.network, testCase.interfaceIndex, service); err != nil {
						t.Fatal(err)
					}

					client, err := net.DialUDP(family.network, nil, listener.LocalAddr().(*net.UDPAddr))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = client.Close() })
					for _, question := range []dns.Question{
						{Name: ServiceName + ".local.", Qtype: dns.TypePTR, Qclass: dns.ClassINET | 1<<15},
						{Name: service.HostName, Qtype: dns.TypeA, Qclass: dns.ClassINET | 1<<15},
					} {
						query := &dns.Msg{MsgHdr: dns.MsgHdr{Id: 42}, Question: []dns.Question{question}}
						packet, err := query.Pack()
						if err != nil {
							t.Fatal(err)
						}
						if _, err = client.Write(packet); err != nil {
							t.Fatal(err)
						}
						timeout := 2 * time.Second
						if !testCase.wantResponse {
							timeout = 100 * time.Millisecond
						}
						if err = client.SetReadDeadline(time.Now().Add(timeout)); err != nil {
							t.Fatal(err)
						}
						buffer := make([]byte, 1500)
						size, err := client.Read(buffer)
						if !testCase.wantResponse {
							var timeoutErr net.Error
							if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() {
								t.Fatalf("expected no response on a foreign interface, got %d bytes, error %v", size, err)
							}
							continue
						}
						if err != nil {
							t.Fatal(err)
						}
						var response dns.Msg
						if err = response.Unpack(buffer[:size]); err != nil {
							t.Fatal(err)
						}
						if !response.Response || !response.Authoritative || response.Id != query.Id {
							t.Fatalf("unexpected response header: %+v", response.MsgHdr)
						}
						var addresses []net.IP
						for _, answer := range response.Answer {
							if address, ok := answer.(*dns.A); ok {
								addresses = append(addresses, address.A)
							}
						}
						if len(addresses) != 1 || !addresses[0].Equal(serviceIP) {
							t.Fatalf("unexpected addresses in response: %v", addresses)
						}
					}
				})
			}
		})
	}
}
