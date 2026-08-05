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
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPeerChunkTransfer(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	server := newTestManager(t, key, "main", nil)
	client := newTestManager(t, key, "main", nil)
	info := server.DiscoveryInfo()
	if nil == info || !client.AddDiscoveredPeer(info.Instance, "127.0.0.1", info.Port, info.TXT) {
		t.Fatal("expected native peer discovery to succeed")
	}
	if 1 != client.DiscoveredPeerCount() {
		t.Fatalf("unexpected discovered peer count: %d", client.DiscoveredPeerCount())
	}
	if 0 != client.ConnectedPeerCount() {
		t.Fatalf("unexpected connected peer count before authentication: %d", client.ConnectedPeerCount())
	}

	data := []byte("encrypted and compressed chunk")
	hash := sha1.Sum(data)
	id := hex.EncodeToString(hash[:])
	path := server.chunkPath(id)
	if err := os.MkdirAll(filepath.Dir(path), 0755); nil != err {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); nil != err {
		t.Fatal(err)
	}

	found, err := client.HasChunks([]string{id, "invalid"})
	if nil != err {
		t.Fatal(err)
	}
	if !found[id] || found["invalid"] {
		t.Fatalf("unexpected peer chunks: %+v", found)
	}
	downloaded, err := client.DownloadChunk(id)
	if nil != err {
		t.Fatal(err)
	}
	if !bytes.Equal(data, downloaded) {
		t.Fatalf("unexpected chunk data: %q", downloaded)
	}
	if 1 != client.ConnectedPeerCount() {
		t.Fatalf("unexpected connected peer count: %d", client.ConnectedPeerCount())
	}
}

func TestPeerRejectsDifferentRepoKey(t *testing.T) {
	server := newTestManager(t, []byte("0123456789abcdef0123456789abcdef"), "main", nil)
	client := newTestManager(t, []byte("abcdef0123456789abcdef0123456789"), "main", nil)
	current := addTestPeer(client, server)
	if err := client.ensureSession(current); nil == err {
		t.Fatal("expected session authentication failure")
	}
}

func TestPeerRejectsDifferentScope(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	server := newTestManager(t, key, "main", nil)
	client := newTestManager(t, key, "other", nil)
	current := addTestPeer(client, server)
	if err := client.ensureSession(current); nil == err {
		t.Fatal("expected scope authentication failure")
	}
}

func TestPeerCommitHint(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	hints := make(chan string, 1)
	server := newTestManager(t, key, "main", func(latestID string) {
		hints <- latestID
	})
	client := newTestManager(t, key, "main", nil)
	current := addTestPeer(client, server)
	if err := client.ensureSession(current); nil != err {
		t.Fatal(err)
	}
	if 1 != server.ConnectedPeerCount() {
		t.Fatalf("unexpected inbound peer count: %d", server.ConnectedPeerCount())
	}

	latestHash := sha1.Sum([]byte("latest index"))
	latestID := hex.EncodeToString(latestHash[:])
	client.NotifyCloudCommit(latestID)
	select {
	case received := <-hints:
		if latestID != received {
			t.Fatalf("unexpected latest ID: %s", received)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for commit hint")
	}
}

func TestObjectIDValidation(t *testing.T) {
	if !validObjectID("0123456789abcdef0123456789abcdef01234567") {
		t.Fatal("expected valid object ID")
	}
	for _, id := range []string{
		"0123456789ABCDEF0123456789ABCDEF01234567",
		"0123456789abcdef0123456789abcdef0123456",
		"0123456789abcdef0123456789abcdef0123456g",
	} {
		if validObjectID(id) {
			t.Fatalf("expected invalid object ID: %s", id)
		}
	}
}

func TestNativeDiscoveryValidation(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	server := newTestManager(t, key, "main", nil)
	client := newTestManager(t, key, "main", nil)
	info := server.DiscoveryInfo()
	if client.AddDiscoveredPeer(info.Instance, "203.0.113.1", info.Port, info.TXT) {
		t.Fatal("expected public peer address to be rejected")
	}
	invalidTXT := map[string]string{"v": info.TXT["v"], "f": info.TXT["f"], "scope": "invalid"}
	if client.AddDiscoveredPeer(info.Instance, "127.0.0.1", info.Port, invalidTXT) {
		t.Fatal("expected invalid discovery scope to be rejected")
	}
}

func TestGroupDiscoveryTargets(t *testing.T) {
	wlanIP := net.ParseIP("192.168.1.2")
	ethernetIP := net.ParseIP("10.0.0.2")
	targets := groupDiscoveryTargets([]net.IP{wlanIP, ethernetIP}, []networkInterface{
		{
			iface: net.Interface{Index: 1, Name: "wlan", Flags: net.FlagUp},
			addrs: []net.Addr{&net.IPNet{IP: wlanIP, Mask: net.CIDRMask(24, 32)}},
		},
		{
			iface: net.Interface{Index: 2, Name: "ethernet", Flags: net.FlagUp},
			addrs: []net.Addr{&net.IPNet{IP: ethernetIP, Mask: net.CIDRMask(24, 32)}},
		},
	})
	if 2 != len(targets) {
		t.Fatalf("unexpected discovery target count: %d", len(targets))
	}
	if "wlan" != targets[0].iface.Name || 1 != len(targets[0].ips) || !targets[0].ips[0].Equal(wlanIP) {
		t.Fatalf("unexpected WLAN discovery target: %+v", targets[0])
	}
	if "ethernet" != targets[1].iface.Name || 1 != len(targets[1].ips) || !targets[1].ips[0].Equal(ethernetIP) {
		t.Fatalf("unexpected Ethernet discovery target: %+v", targets[1])
	}
}

func newTestManager(t *testing.T, key []byte, scope string, onCommitHint func(string)) *Manager {
	t.Helper()
	tempDir := t.TempDir()
	currentIdentity, err := loadOrCreateIdentity(filepath.Join(tempDir, "identity.json"))
	if nil != err {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	manager := &Manager{
		config: Config{
			RepoPath:          filepath.Join(tempDir, "repo"),
			RepoKey:           key,
			Scope:             scope,
			DeviceName:        "Test Device",
			DeviceOS:          "test",
			AppVersion:        "test",
			MaxConcurrentReqs: 4,
			OnCommitHint:      onCommitHint,
		},
		identity:     currentIdentity,
		instance:     "siyuan-" + currentIdentity.id[:16],
		discoveryKey: deriveKey(key, "siyuan-lan-sync-discovery-v1"),
		authKey:      deriveKey(key, "siyuan-lan-sync-auth-v1"),
		ctx:          ctx,
		cancel:       cancel,
		peers:        map[string]*peer{},
		routes:       map[string][]*peer{},
		sessions:     map[string]*serverSession{},
	}
	manager.scopeID = calculateScopeID(manager.discoveryKey, scope)
	if err = manager.startServer(); nil != err {
		t.Fatal(err)
	}
	t.Cleanup(manager.Stop)
	return manager
}

func addTestPeer(client, server *Manager) *peer {
	port := server.listener.Addr().(*net.TCPAddr).Port
	current := &peer{
		key:      net.JoinHostPort("127.0.0.1", "0"),
		instance: server.identity.id,
		address:  "127.0.0.1",
		port:     port,
		lastSeen: time.Now(),
	}
	client.peerMu.Lock()
	client.peers[current.key] = current
	client.peerMu.Unlock()
	return current
}
