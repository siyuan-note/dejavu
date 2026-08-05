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
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/mdns"
	"github.com/siyuan-note/dejavu"
	"github.com/siyuan-note/logging"
)

var _ dejavu.ChunkSource = (*Manager)(nil)

type Config struct {
	RepoPath          string
	IdentityPath      string
	RepoKey           []byte
	Scope             string
	DeviceName        string
	DeviceOS          string
	AppVersion        string
	IPs               []net.IP
	IPsProvider       func() []net.IP
	MaxConcurrentReqs int
	NativeDiscovery   bool
	OnCommitHint      func(latestID string)
}

type DiscoveryInfo struct {
	Instance    string            `json:"instance"`
	ServiceType string            `json:"serviceType"`
	Port        int               `json:"port"`
	TXT         map[string]string `json:"txt"`
}

type Manager struct {
	config       Config
	identity     *identity
	discoveryKey []byte
	authKey      []byte
	scopeID      []byte
	instance     string

	ctx    context.Context
	cancel context.CancelFunc

	listener    net.Listener
	httpServer  *http.Server
	mdnsMu      sync.Mutex
	mdnsServers []*mdns.Server

	peerMu sync.RWMutex
	peers  map[string]*peer
	routes map[string][]*peer

	sessionMu sync.Mutex
	sessions  map[string]*serverSession

	stopOnce sync.Once
}

type peer struct {
	mu sync.Mutex

	key      string
	instance string
	address  string
	port     int
	lastSeen time.Time

	deviceID     string
	deviceName   string
	deviceOS     string
	appVersion   string
	token        string
	tokenExpires time.Time
	client       *http.Client
}

type serverSession struct {
	peerID   string
	certHash string
	expires  time.Time
}

func Start(config Config) (ret *Manager, err error) {
	if 1 > len(config.RepoKey) {
		return nil, errors.New("LAN sync repo key is empty")
	}
	if config.MaxConcurrentReqs < 1 {
		config.MaxConcurrentReqs = 16
	}
	if 128 < config.MaxConcurrentReqs {
		config.MaxConcurrentReqs = 128
	}
	identity, err := loadOrCreateIdentity(config.IdentityPath)
	if nil != err {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	randomInstance := make([]byte, 8)
	if _, err = rand.Read(randomInstance); nil != err {
		cancel()
		return nil, err
	}
	ret = &Manager{
		config:       config,
		identity:     identity,
		discoveryKey: deriveKey(config.RepoKey, "siyuan-lan-sync-discovery-v1"),
		authKey:      deriveKey(config.RepoKey, "siyuan-lan-sync-auth-v1"),
		instance:     "siyuan-" + hex.EncodeToString(randomInstance),
		ctx:          ctx,
		cancel:       cancel,
		peers:        map[string]*peer{},
		routes:       map[string][]*peer{},
		sessions:     map[string]*serverSession{},
	}
	ret.scopeID = calculateScopeID(ret.discoveryKey, config.Scope)
	if err = ret.startServer(); nil != err {
		cancel()
		return nil, err
	}
	if !config.NativeDiscovery {
		if err = ret.refreshAdvertisement(); nil != err {
			logging.LogWarnf("start LAN sync discovery advertisement failed: %s", err)
		}
		go ret.discoveryLoop()
		go ret.advertisementLoop()
	}
	logging.LogInfof("LAN sync service started [device=%s, port=%d]", ret.identity.id, ret.listener.Addr().(*net.TCPAddr).Port)
	return
}

func (manager *Manager) DiscoveryInfo() *DiscoveryInfo {
	if nil == manager.listener {
		return nil
	}
	return &DiscoveryInfo{
		Instance:    manager.instance,
		ServiceType: ServiceName + ".",
		Port:        manager.listener.Addr().(*net.TCPAddr).Port,
		TXT: map[string]string{
			"v":     strconv.Itoa(ProtocolVersion),
			"f":     strconv.Itoa(ObjectFormatVersion),
			"scope": calculateDiscoveryTag(manager.discoveryKey, manager.scopeID, time.Now()),
		},
	}
}

func (manager *Manager) AddDiscoveredPeer(instance, address string, port int, txt map[string]string) bool {
	return manager.addDiscoveredPeerInfo(instance, address, port, txt)
}

func (manager *Manager) Stop() {
	manager.stopOnce.Do(func() {
		manager.cancel()
		manager.peerMu.RLock()
		peers := make([]*peer, 0, len(manager.peers))
		for _, current := range manager.peers {
			peers = append(peers, current)
		}
		manager.peerMu.RUnlock()
		for _, current := range peers {
			manager.clearPeerSession(current)
		}
		manager.stopAdvertisements()
		if nil != manager.httpServer {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = manager.httpServer.Shutdown(ctx)
			cancel()
		}
		if nil != manager.listener {
			_ = manager.listener.Close()
		}
		logging.LogInfof("LAN sync service stopped")
	})
}

func (manager *Manager) Name() string {
	return "LAN peer"
}

func (manager *Manager) GetConcurrentReqs() int {
	return manager.config.MaxConcurrentReqs
}

func (manager *Manager) DiscoveredPeerCount() int {
	instances := map[string]bool{}
	cutoff := time.Now().Add(-time.Minute)
	manager.peerMu.RLock()
	for _, current := range manager.peers {
		current.mu.Lock()
		instance := current.instance
		lastSeen := current.lastSeen
		current.mu.Unlock()
		if "" != instance && lastSeen.After(cutoff) {
			instances[instance] = true
		}
	}
	manager.peerMu.RUnlock()
	return len(instances)
}

func (manager *Manager) ConnectedPeerCount() int {
	deviceIDs := map[string]bool{}
	manager.peerMu.RLock()
	for _, current := range manager.peers {
		current.mu.Lock()
		if "" != current.deviceID && "" != current.token && time.Now().Before(current.tokenExpires) {
			deviceIDs[current.deviceID] = true
		}
		current.mu.Unlock()
	}
	manager.peerMu.RUnlock()
	manager.sessionMu.Lock()
	manager.cleanupExpiredSessionsLocked(time.Now())
	for _, session := range manager.sessions {
		deviceIDs[session.peerID] = true
	}
	manager.sessionMu.Unlock()
	return len(deviceIDs)
}

func (manager *Manager) cleanupExpiredSessionsLocked(now time.Time) {
	for token, session := range manager.sessions {
		if now.After(session.expires) {
			delete(manager.sessions, token)
		}
	}
}

func (manager *Manager) tlsServerConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{manager.identity.certificate},
		ClientAuth:   tls.RequireAnyClientCert,
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2", "http/1.1"},
	}
}
