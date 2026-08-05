// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package lansync

import (
	"errors"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/siyuan-note/logging"
)

var mdnsQueryLogger = log.New(io.Discard, "", 0)

func (manager *Manager) refreshAdvertisement() (err error) {
	manager.mdnsMu.Lock()
	defer manager.mdnsMu.Unlock()
	for _, server := range manager.mdnsServers {
		_ = server.Shutdown()
	}
	manager.mdnsServers = nil
	info := manager.DiscoveryInfo()
	ips := manager.config.IPs
	if nil != manager.config.IPsProvider {
		ips = manager.config.IPsProvider()
	}
	if 1 > len(ips) {
		return errors.New("no private network address is available")
	}
	txt := []string{
		"v=" + info.TXT["v"],
		"f=" + info.TXT["f"],
		"scope=" + info.TXT["scope"],
	}
	var lastErr error
	for _, target := range discoveryTargets(ips) {
		service, serviceErr := mdns.NewMDNSService(manager.instance, ServiceName, "", "", info.Port, target.ips, txt)
		if nil != serviceErr {
			lastErr = serviceErr
			continue
		}
		server, serverErr := mdns.NewServer(&mdns.Config{Zone: service, Iface: target.iface})
		if nil != serverErr {
			lastErr = serverErr
			continue
		}
		manager.mdnsServers = append(manager.mdnsServers, server)
	}
	if 0 < len(manager.mdnsServers) {
		return nil
	}
	if nil != lastErr {
		return lastErr
	}
	return errors.New("no multicast interface is available")
}

func (manager *Manager) advertisementLoop() {
	delay := discoveryWindow
	manager.mdnsMu.Lock()
	if 0 == len(manager.mdnsServers) {
		delay = 30 * time.Second
	}
	manager.mdnsMu.Unlock()
	for {
		timer := time.NewTimer(delay)
		select {
		case <-manager.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			if err := manager.refreshAdvertisement(); nil != err {
				logging.LogWarnf("refresh LAN sync discovery advertisement failed: %s", err)
				delay = 30 * time.Second
			} else {
				delay = discoveryWindow
			}
		}
	}
}

func (manager *Manager) discoveryLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	manager.browseOnce()
	for {
		select {
		case <-manager.ctx.Done():
			return
		case <-ticker.C:
			manager.browseOnce()
		}
	}
}

func (manager *Manager) peerCleanupLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-manager.ctx.Done():
			return
		case <-ticker.C:
			manager.removeStalePeers()
		}
	}
}

func (manager *Manager) browseOnce() {
	ips := manager.config.IPs
	if nil != manager.config.IPsProvider {
		ips = manager.config.IPsProvider()
	}
	targets := discoveryTargets(ips)
	waitGroup := sync.WaitGroup{}
	for _, target := range targets {
		waitGroup.Add(1)
		go func(iface *net.Interface) {
			defer waitGroup.Done()
			manager.browseInterface(iface)
		}(target.iface)
	}
	waitGroup.Wait()
}

func (manager *Manager) browseInterface(iface *net.Interface) {
	entries := make(chan *mdns.ServiceEntry, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for entry := range entries {
			manager.addDiscoveredPeer(entry)
		}
	}()
	err := mdns.Query(&mdns.QueryParam{
		Service:             ServiceName,
		Domain:              "local",
		Timeout:             2 * time.Second,
		Interface:           iface,
		Entries:             entries,
		WantUnicastResponse: true,
		DisableIPv6:         true,
		Logger:              mdnsQueryLogger,
	})
	close(entries)
	<-done
	if nil != err && nil == manager.ctx.Err() {
		logging.LogDebugf("browse LAN sync peers failed: %s", err)
	}
}

type discoveryTarget struct {
	iface *net.Interface
	ips   []net.IP
}

type networkInterface struct {
	iface net.Interface
	addrs []net.Addr
}

func discoveryTargets(ips []net.IP) []discoveryTarget {
	interfaces, err := net.Interfaces()
	if nil != err {
		return []discoveryTarget{{ips: ips}}
	}
	candidates := make([]networkInterface, 0, len(interfaces))
	for _, current := range interfaces {
		addresses, addressErr := current.Addrs()
		if nil == addressErr {
			candidates = append(candidates, networkInterface{iface: current, addrs: addresses})
		}
	}
	return groupDiscoveryTargets(ips, candidates)
}

func groupDiscoveryTargets(ips []net.IP, interfaces []networkInterface) (ret []discoveryTarget) {
	targetIndexes := map[int]int{}
	for _, ip := range ips {
		for i := range interfaces {
			current := &interfaces[i]
			if 0 == current.iface.Flags&net.FlagUp || 0 != current.iface.Flags&net.FlagLoopback {
				continue
			}
			if !interfaceHasIP(current.addrs, ip) {
				continue
			}
			targetIndex, exists := targetIndexes[current.iface.Index]
			if !exists {
				targetIndex = len(ret)
				targetIndexes[current.iface.Index] = targetIndex
				ret = append(ret, discoveryTarget{iface: &current.iface})
			}
			ret[targetIndex].ips = append(ret[targetIndex].ips, ip)
			break
		}
	}
	if 0 == len(ret) {
		ret = append(ret, discoveryTarget{ips: ips})
	}
	return
}

func interfaceHasIP(addresses []net.Addr, target net.IP) bool {
	for _, address := range addresses {
		var current net.IP
		switch value := address.(type) {
		case *net.IPNet:
			current = value.IP
		case *net.IPAddr:
			current = value.IP
		}
		if nil != current && current.Equal(target) {
			return true
		}
	}
	return false
}

func (manager *Manager) stopAdvertisements() {
	manager.mdnsMu.Lock()
	defer manager.mdnsMu.Unlock()
	for _, server := range manager.mdnsServers {
		_ = server.Shutdown()
	}
	manager.mdnsServers = nil
}

func (manager *Manager) addDiscoveredPeer(entry *mdns.ServiceEntry) {
	if nil == entry || strings.HasPrefix(entry.Name, manager.instance+".") || entry.Port < 1 {
		return
	}
	fields := map[string]string{}
	for _, field := range entry.InfoFields {
		parts := strings.SplitN(field, "=", 2)
		if 2 == len(parts) {
			fields[parts[0]] = parts[1]
		}
	}
	addressString := ""
	if nil != entry.AddrV4 {
		addressString = entry.AddrV4.String()
	} else if nil != entry.AddrV6IPAddr {
		addressString = entry.AddrV6IPAddr.String()
	} else if nil != entry.AddrV6 {
		addressString = entry.AddrV6.String()
	}
	manager.addDiscoveredPeerInfo(entry.Name, addressString, entry.Port, fields)
}

func (manager *Manager) addDiscoveredPeerInfo(instance, address string, port int, fields map[string]string) bool {
	if "" == instance || strings.HasPrefix(instance, manager.instance+".") || instance == manager.instance || port < 1 ||
		65535 < port || strconv.Itoa(ProtocolVersion) != fields["v"] ||
		strconv.Itoa(ObjectFormatVersion) != fields["f"] || !manager.matchesDiscoveryTag(fields["scope"]) {
		return false
	}
	ipAddress := address
	if index := strings.LastIndex(ipAddress, "%"); 0 < index {
		ipAddress = ipAddress[:index]
	}
	ip := net.ParseIP(ipAddress)
	if nil == ip || !(ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast()) {
		return false
	}
	key := net.JoinHostPort(address, strconv.Itoa(port))
	manager.peerMu.Lock()
	current := manager.peers[key]
	if nil == current {
		current = &peer{key: key, instance: instance, address: address, port: port}
		manager.peers[key] = current
	}
	current.mu.Lock()
	current.lastSeen = time.Now()
	current.mu.Unlock()
	manager.peerMu.Unlock()
	return true
}

func (manager *Manager) matchesDiscoveryTag(tag string) bool {
	now := time.Now()
	for offset := -1; offset <= 1; offset++ {
		if calculateDiscoveryTag(manager.discoveryKey, manager.scopeID, now.Add(time.Duration(offset)*discoveryWindow)) == tag {
			return true
		}
	}
	return false
}

func (manager *Manager) removeStalePeers() {
	cutoff := time.Now().Add(-time.Minute)
	manager.removePeers(func(current *peer) bool {
		return current.lastSeen.Before(cutoff)
	})
}

func (manager *Manager) removePeers(matches func(current *peer) bool) (ret []*peer) {
	manager.peerMu.Lock()
	for key, current := range manager.peers {
		current.mu.Lock()
		remove := matches(current)
		current.mu.Unlock()
		if remove {
			delete(manager.peers, key)
			ret = append(ret, current)
		}
	}
	if 0 < len(ret) {
		for id, routes := range manager.routes {
			kept := routes[:0]
			for _, route := range routes {
				remove := false
				for _, current := range ret {
					if route == current {
						remove = true
						break
					}
				}
				if !remove {
					kept = append(kept, route)
				}
			}
			if 0 == len(kept) {
				delete(manager.routes, id)
			} else {
				manager.routes[id] = kept
			}
		}
	}
	manager.peerMu.Unlock()

	removedDeviceIDs := map[string]bool{}
	for _, current := range ret {
		current.mu.Lock()
		if "" != current.deviceID {
			removedDeviceIDs[current.deviceID] = true
		}
		current.mu.Unlock()
		manager.clearPeerSession(current)
	}
	if 0 < len(removedDeviceIDs) {
		manager.sessionMu.Lock()
		for token, session := range manager.sessions {
			if removedDeviceIDs[session.peerID] {
				delete(manager.sessions, token)
			}
		}
		manager.sessionMu.Unlock()
	}
	return
}
