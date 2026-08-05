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
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/siyuan-note/logging"
)

func (manager *Manager) refreshAdvertisement() (err error) {
	manager.mdnsMu.Lock()
	defer manager.mdnsMu.Unlock()
	if nil != manager.mdnsServer {
		_ = manager.mdnsServer.Shutdown()
		manager.mdnsServer = nil
	}
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
	service, err := mdns.NewMDNSService(manager.instance, ServiceName, "", "", info.Port, ips, txt)
	if nil != err {
		return err
	}
	manager.mdnsServer, err = mdns.NewServer(&mdns.Config{Zone: service})
	return
}

func (manager *Manager) advertisementLoop() {
	delay := discoveryWindow
	manager.mdnsMu.Lock()
	if nil == manager.mdnsServer {
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
			manager.removeStalePeers()
		}
	}
}

func (manager *Manager) browseOnce() {
	entries := make(chan *mdns.ServiceEntry, 32)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for entry := range entries {
			manager.addDiscoveredPeer(entry)
		}
	}()
	err := mdns.Query(&mdns.QueryParam{
		Service: ServiceName,
		Domain:  "local",
		Timeout: 2 * time.Second,
		Entries: entries,
	})
	close(entries)
	<-done
	if nil != err && nil == manager.ctx.Err() {
		logging.LogDebugf("browse LAN sync peers failed: %s", err)
	}
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
	manager.peerMu.Lock()
	for key, current := range manager.peers {
		current.mu.Lock()
		stale := current.lastSeen.Before(cutoff)
		current.mu.Unlock()
		if stale {
			delete(manager.peers, key)
		}
	}
	manager.peerMu.Unlock()
}
