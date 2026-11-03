// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

package lansync

import (
	"context"
	"net"
	"strings"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/miekg/dns"
	"golang.org/x/net/ipv4"
)

func validDiscoveryHost(host string) bool {
	_, valid := dns.IsDomainName(host)
	return valid && len(host) > 1 && strings.HasSuffix(host, ".")
}

func (manager *Manager) resolveDiscoveryHosts(iface *net.Interface, hosts map[string][]*mdns.ServiceEntry) {
	if len(hosts) == 0 || manager.ctx.Err() != nil {
		return
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		return
	}
	defer conn.Close()
	packet := ipv4.NewPacketConn(conn)
	if iface != nil {
		if err = packet.SetMulticastInterface(iface); err != nil {
			return
		}
	}
	if err = packet.SetMulticastTTL(255); err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(manager.ctx, 2*time.Second)
	defer cancel()
	manager.queryDiscoveryAddresses(ctx, conn, &net.UDPAddr{IP: net.IPv4(224, 0, 0, 251), Port: 5353}, hosts)
}

func (manager *Manager) queryDiscoveryAddresses(ctx context.Context, conn *net.UDPConn, destination *net.UDPAddr,
	hosts map[string][]*mdns.ServiceEntry) {
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	for host := range hosts {
		query := new(dns.Msg)
		// 临时端口接收单播应答，避免与正在运行的 mDNS 服务争用 5353 端口。
		query.Question = []dns.Question{{Name: host, Qtype: dns.TypeA, Qclass: dns.ClassINET | 1<<15}}
		data, packErr := query.Pack()
		if packErr != nil {
			continue
		}
		if _, err := conn.WriteToUDP(data, destination); err != nil {
			return
		}
	}
	buffer := make([]byte, 65535)
	seen := map[string]bool{}
	for {
		n, _, readErr := conn.ReadFromUDP(buffer)
		if readErr != nil {
			return
		}
		response := new(dns.Msg)
		if response.Unpack(buffer[:n]) != nil || !response.Response ||
			response.Opcode != dns.OpcodeQuery || response.Rcode != dns.RcodeSuccess {
			continue
		}
		manager.addDiscoveryAddresses(response, hosts, seen)
	}
}

func (manager *Manager) addDiscoveryAddresses(response *dns.Msg, hosts map[string][]*mdns.ServiceEntry, seen map[string]bool) {
	for _, record := range append(response.Answer, response.Extra...) {
		address, ok := record.(*dns.A)
		if !ok || address.Hdr.Ttl == 0 || address.Hdr.Class&0x7fff != dns.ClassINET {
			continue
		}
		for _, entry := range hosts[strings.ToLower(address.Hdr.Name)] {
			key := entry.Name + ":" + address.A.String()
			if seen[key] || len(seen) >= 64 {
				continue
			}
			seen[key] = true
			candidate := *entry
			candidate.AddrV4 = address.A
			manager.addDiscoveredPeer(&candidate)
		}
	}
}
