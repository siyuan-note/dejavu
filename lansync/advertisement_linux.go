// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !android

package lansync

import (
	"errors"
	"net"

	"github.com/hashicorp/mdns"
	"github.com/miekg/dns"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

const mdnsPort = 5353

type linuxMDNSAdvertisement struct {
	sockets []*net.UDPConn
}

func startMDNSAdvertisement(zone mdns.Zone, iface *net.Interface) (mdnsAdvertisement, error) {
	if iface == nil {
		return mdns.NewServer(&mdns.Config{Zone: zone})
	}

	server := &linuxMDNSAdvertisement{}
	var listenErrors []error
	for _, group := range []struct {
		network string
		ip      net.IP
	}{
		{"udp4", net.IPv4(224, 0, 0, 251)},
		{"udp6", net.ParseIP("ff02::fb")},
	} {
		conn, err := net.ListenMulticastUDP(group.network, iface, &net.UDPAddr{IP: group.ip, Port: mdnsPort})
		if err != nil {
			listenErrors = append(listenErrors, err)
			continue
		}
		if err = startLinuxMDNSReceiver(conn, group.network, iface.Index, zone); err != nil {
			_ = conn.Close()
			listenErrors = append(listenErrors, err)
			continue
		}
		server.sockets = append(server.sockets, conn)
	}
	if len(server.sockets) == 0 {
		return nil, errors.Join(listenErrors...)
	}
	return server, nil
}

func (server *linuxMDNSAdvertisement) Shutdown() error {
	var closeErrors []error
	for _, conn := range server.sockets {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func startLinuxMDNSReceiver(conn *net.UDPConn, network string, interfaceIndex int, zone mdns.Zone) error {
	if network == "udp4" {
		if err := ipv4.NewPacketConn(conn).SetControlMessage(ipv4.FlagInterface, true); err != nil {
			return err
		}
		outgoing := (&ipv4.ControlMessage{IfIndex: interfaceIndex}).Marshal()
		go serveLinuxMDNS(conn, zone, ipv4.NewControlMessage(ipv4.FlagInterface), outgoing, func(data []byte) bool {
			var control ipv4.ControlMessage
			return control.Parse(data) == nil && control.IfIndex == interfaceIndex
		})
		return nil
	}

	if err := ipv6.NewPacketConn(conn).SetControlMessage(ipv6.FlagInterface, true); err != nil {
		return err
	}
	outgoing := (&ipv6.ControlMessage{IfIndex: interfaceIndex}).Marshal()
	go serveLinuxMDNS(conn, zone, ipv6.NewControlMessage(ipv6.FlagInterface), outgoing, func(data []byte) bool {
		var control ipv6.ControlMessage
		return control.Parse(data) == nil && control.IfIndex == interfaceIndex
	})
	return nil
}

func serveLinuxMDNS(conn *net.UDPConn, zone mdns.Zone, incoming, outgoing []byte, matchesInterface func([]byte) bool) {
	data := make([]byte, 65535)
	for {
		size, controlSize, _, from, err := conn.ReadMsgUDP(data, incoming)
		if err != nil {
			return
		}
		// Linux 可能将同一组播查询投递给在不同网卡上加入组播组的多个套接字。
		if !matchesInterface(incoming[:controlSize]) {
			continue
		}
		var query dns.Msg
		if query.Unpack(data[:size]) != nil || query.Response || query.Opcode != dns.OpcodeQuery ||
			query.Rcode != dns.RcodeSuccess || query.Truncated {
			continue
		}
		for _, response := range linuxMDNSResponses(&query, zone) {
			if len(response.Answer) == 0 {
				continue
			}
			packet, err := response.Pack()
			if err == nil {
				_, _, _ = conn.WriteMsgUDP(packet, outgoing, from)
			}
		}
	}
}

func linuxMDNSResponses(query *dns.Msg, zone mdns.Zone) (responses [2]dns.Msg) {
	// 局域网发现和地址补查都可通过单播接收应答，查询的 QU 标志决定应答是否回显事务 ID。
	header := dns.MsgHdr{Response: true, Authoritative: true}
	responses[0] = dns.Msg{MsgHdr: header, Compress: true}
	header.Id = query.Id
	responses[1] = dns.Msg{MsgHdr: header, Compress: true}
	for _, question := range query.Question {
		index := question.Qclass >> 15
		responses[index].Answer = append(responses[index].Answer, zone.Records(question)...)
	}
	return
}
