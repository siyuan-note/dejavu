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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/siyuan-note/logging"
)

func (manager *Manager) HasChunks(ids []string) (ret map[string]bool, err error) {
	ret = map[string]bool{}
	if 1 > len(ids) {
		return
	}
	manager.peerMu.RLock()
	peers := make([]*peer, 0, len(manager.peers))
	for _, current := range manager.peers {
		peers = append(peers, current)
	}
	manager.peerMu.RUnlock()
	sort.Slice(peers, func(i, j int) bool {
		peers[i].mu.Lock()
		iLastSeen := peers[i].lastSeen
		peers[i].mu.Unlock()
		peers[j].mu.Lock()
		jLastSeen := peers[j].lastSeen
		peers[j].mu.Unlock()
		return iLastSeen.After(jLastSeen)
	})

	routes := map[string][]*peer{}
	routesMu := sync.Mutex{}
	var queryErrors []error
	successfulQueries := 0
	semaphore := make(chan struct{}, manager.config.MaxConcurrentReqs)
	waitGroup := sync.WaitGroup{}
	for start := 0; start < len(ids); start += maxHasChunks {
		end := start + maxHasChunks
		if len(ids) < end {
			end = len(ids)
		}
		batch := ids[start:end]
		for _, current := range peers {
			waitGroup.Add(1)
			go func(target *peer, queryIDs []string) {
				defer waitGroup.Done()
				select {
				case semaphore <- struct{}{}:
					defer func() { <-semaphore }()
				case <-manager.ctx.Done():
					return
				}
				found, queryErr := manager.queryPeerChunks(target, queryIDs)
				if nil != queryErr {
					routesMu.Lock()
					queryErrors = append(queryErrors, fmt.Errorf("query LAN peer [%s] failed: %w",
						net.JoinHostPort(target.address, fmt.Sprintf("%d", target.port)), queryErr))
					routesMu.Unlock()
					return
				}
				routesMu.Lock()
				successfulQueries++
				for _, id := range found {
					ret[id] = true
					routes[id] = append(routes[id], target)
				}
				routesMu.Unlock()
			}(current, batch)
		}
	}
	waitGroup.Wait()
	manager.peerMu.Lock()
	manager.routes = routes
	manager.peerMu.Unlock()
	if 0 < len(queryErrors) {
		queryErr := fmt.Errorf("query LAN peers failed: %w", errors.Join(queryErrors...))
		if 0 == successfulQueries {
			err = queryErr
		} else {
			logging.LogWarnf("some LAN peers are unavailable: %s", queryErr)
		}
	}
	return
}

func (manager *Manager) DownloadChunk(id string) (data []byte, err error) {
	return manager.DownloadChunkValidated(id, nil)
}

func (manager *Manager) DownloadChunkValidated(id string, validate func(data []byte) error) (data []byte, err error) {
	manager.peerMu.RLock()
	routes := append([]*peer(nil), manager.routes[id]...)
	manager.peerMu.RUnlock()
	if 1 > len(routes) {
		return nil, errors.New("LAN chunk source not found")
	}
	var lastErr error
	for _, current := range routes {
		data, lastErr = manager.downloadPeerChunk(current, id)
		if nil == lastErr && nil != validate {
			lastErr = validate(data)
		}
		if nil == lastErr {
			return data, nil
		}
	}
	if nil == lastErr {
		lastErr = errors.New("LAN chunk source unavailable")
	}
	return nil, lastErr
}

func (manager *Manager) HasObjects(ids []string) (ret map[string]bool, err error) {
	return manager.HasChunks(ids)
}

func (manager *Manager) DownloadObjectValidated(id string, validate func(data []byte) error) (data []byte, err error) {
	return manager.DownloadChunkValidated(id, validate)
}

func (manager *Manager) NotifyCloudCommit(latestID string) {
	if !validObjectID(latestID) {
		return
	}
	manager.peerMu.RLock()
	peers := make([]*peer, 0, len(manager.peers))
	for _, current := range manager.peers {
		peers = append(peers, current)
	}
	manager.peerMu.RUnlock()
	for _, current := range peers {
		go func(target *peer) {
			_ = manager.postPeerJSON(target, "/peer-sync/v1/commits/hint", &commitHintRequest{LatestID: latestID}, nil)
		}(current)
	}
}

func (manager *Manager) queryPeerChunks(current *peer, ids []string) (ret []string, err error) {
	response := &hasChunksResponse{}
	err = manager.postPeerJSON(current, "/peer-sync/v1/chunks/has", &hasChunksRequest{IDs: ids}, response)
	if nil != err {
		return nil, err
	}
	for _, id := range response.IDs {
		if validObjectID(id) {
			ret = append(ret, id)
		}
	}
	return
}

func (manager *Manager) downloadPeerChunk(current *peer, id string) (data []byte, err error) {
	if err = manager.ensureSession(current); nil != err {
		return nil, err
	}
	current.mu.Lock()
	client, token, address, port := current.client, current.token, current.address, current.port
	current.mu.Unlock()
	if nil == client || "" == token {
		return nil, errors.New("LAN peer session unavailable")
	}
	request, err := http.NewRequestWithContext(manager.ctx, http.MethodGet,
		peerURL(address, port)+"/peer-sync/v1/chunks/"+id, nil)
	if nil != err {
		return nil, err
	}
	request.Header.Set("Authorization", "PeerSession "+token)
	response, err := client.Do(request)
	if nil != err {
		manager.clearPeerSession(current)
		return nil, err
	}
	defer response.Body.Close()
	if http.StatusOK != response.StatusCode {
		if http.StatusUnauthorized == response.StatusCode {
			manager.clearPeerSession(current)
		}
		return nil, fmt.Errorf("LAN chunk request failed with status %d", response.StatusCode)
	}
	return io.ReadAll(io.LimitReader(response.Body, maxChunkSize))
}

func (manager *Manager) postPeerJSON(current *peer, path string, requestBody, responseBody interface{}) (err error) {
	if err = manager.ensureSession(current); nil != err {
		return err
	}
	data, err := json.Marshal(requestBody)
	if nil != err {
		return err
	}
	current.mu.Lock()
	client, token, address, port := current.client, current.token, current.address, current.port
	current.mu.Unlock()
	if nil == client || "" == token {
		return errors.New("LAN peer session unavailable")
	}
	request, err := http.NewRequestWithContext(manager.ctx, http.MethodPost, peerURL(address, port)+path, bytes.NewReader(data))
	if nil != err {
		return err
	}
	request.Header.Set("Authorization", "PeerSession "+token)
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if nil != err {
		manager.clearPeerSession(current)
		return err
	}
	defer response.Body.Close()
	if http.StatusNoContent != response.StatusCode && http.StatusOK != response.StatusCode {
		if http.StatusUnauthorized == response.StatusCode {
			manager.clearPeerSession(current)
		}
		return fmt.Errorf("LAN peer request failed with status %d", response.StatusCode)
	}
	if nil != responseBody {
		return json.NewDecoder(io.LimitReader(response.Body, 512*1024)).Decode(responseBody)
	}
	return nil
}

func (manager *Manager) ensureSession(current *peer) (err error) {
	current.sessionMu.Lock()
	defer current.sessionMu.Unlock()

	current.mu.Lock()
	if current.removed {
		current.mu.Unlock()
		return errors.New("LAN peer was removed")
	}
	if "" != current.token && time.Now().Before(current.tokenExpires) && nil != current.client {
		current.mu.Unlock()
		return nil
	}
	address, port := current.address, current.port
	current.mu.Unlock()

	client := manager.newPeerHTTPClient()
	baseURL := peerURL(address, port)
	identityRequest, err := http.NewRequestWithContext(manager.ctx, http.MethodGet, baseURL+"/peer-sync/v1/identity", nil)
	if nil != err {
		return err
	}
	identityResponse, err := client.Do(identityRequest)
	if nil != err {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(identityResponse.Body, 16*1024))
	identityResponse.Body.Close()
	if http.StatusOK != identityResponse.StatusCode || nil == identityResponse.TLS || 1 > len(identityResponse.TLS.PeerCertificates) {
		return errors.New("invalid LAN peer identity response")
	}
	serverCertHash := sha256.Sum256(identityResponse.TLS.PeerCertificates[0].Raw)
	serverDeviceID, err := deviceIDFromCertificate(identityResponse.TLS.PeerCertificates[0])
	if nil != err {
		return err
	}
	clientNonce := make([]byte, 32)
	if _, err = rand.Read(clientNonce); nil != err {
		return err
	}
	proof := calculateProof(manager.authKey, "client", manager.scopeID, clientNonce, nil, manager.identity.certHash,
		serverCertHash[:])
	openRequest := &sessionOpenRequest{
		ProtocolVersion: ProtocolVersion,
		ObjectFormat:    ObjectFormatVersion,
		Scope:           base64.RawURLEncoding.EncodeToString(manager.scopeID),
		Nonce:           base64.RawURLEncoding.EncodeToString(clientNonce),
		DeviceID:        manager.identity.id,
		DeviceName:      manager.config.DeviceName,
		DeviceOS:        manager.config.DeviceOS,
		AppVersion:      manager.config.AppVersion,
		Proof:           proof,
	}
	requestData, err := json.Marshal(openRequest)
	if nil != err {
		return err
	}
	request, err := http.NewRequestWithContext(manager.ctx, http.MethodPost, baseURL+"/peer-sync/v1/session/open",
		bytes.NewReader(requestData))
	if nil != err {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if nil != err {
		return err
	}
	defer response.Body.Close()
	if http.StatusOK != response.StatusCode {
		return fmt.Errorf("open LAN peer session failed with status %d", response.StatusCode)
	}
	opened := &sessionOpenResponse{}
	if err = json.NewDecoder(io.LimitReader(response.Body, 32*1024)).Decode(opened); nil != err {
		return err
	}
	if ProtocolVersion != opened.ProtocolVersion || ObjectFormatVersion != opened.ObjectFormat || manager.identity.id == opened.DeviceID {
		return errors.New("incompatible LAN peer session")
	}
	if !hmac.Equal([]byte(serverDeviceID), []byte(opened.DeviceID)) {
		return errors.New("invalid LAN peer device identity")
	}
	serverNonce, err := base64.RawURLEncoding.DecodeString(opened.Nonce)
	if nil != err || 32 != len(serverNonce) {
		return errors.New("invalid LAN peer session nonce")
	}
	expectedProof := calculateProof(manager.authKey, "server", manager.scopeID, clientNonce, serverNonce,
		manager.identity.certHash, serverCertHash[:])
	providedProof, err := base64.RawURLEncoding.DecodeString(opened.Proof)
	expectedProofBytes, _ := base64.RawURLEncoding.DecodeString(expectedProof)
	if nil != err || !hmac.Equal(providedProof, expectedProofBytes) {
		return errors.New("invalid LAN peer session proof")
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	if current.removed || current.address != address || current.port != port {
		client.CloseIdleConnections()
		return errors.New("LAN peer changed during authentication")
	}
	current.deviceID = opened.DeviceID
	current.deviceName = opened.DeviceName
	current.deviceOS = opened.DeviceOS
	current.appVersion = opened.AppVersion
	current.token = opened.Token
	current.tokenExpires = time.Now().Add(sessionLifetime)
	current.client = client
	return nil
}

func (manager *Manager) authenticatePeerAsync(current *peer) {
	if nil != manager.ctx.Err() {
		return
	}
	current.mu.Lock()
	if current.removed || current.authenticating ||
		("" != current.token && time.Now().Before(current.tokenExpires) && nil != current.client) {
		current.mu.Unlock()
		return
	}
	current.authenticating = true
	firstAuthentication := "" == current.deviceID
	peerAddress := net.JoinHostPort(current.address, strconv.Itoa(current.port))
	current.mu.Unlock()

	go func() {
		err := manager.ensureSession(current)
		current.mu.Lock()
		current.authenticating = false
		logFailure := nil != err && !current.authFailureLogged
		if nil == err {
			current.authFailureLogged = false
		} else if logFailure {
			current.authFailureLogged = true
		}
		deviceID := current.deviceID
		current.mu.Unlock()

		if nil == err {
			if firstAuthentication {
				logging.LogInfof("authenticated LAN sync peer [device=%s, address=%s]", deviceID, peerAddress)
			}
		} else if logFailure && nil == manager.ctx.Err() {
			logging.LogWarnf("authenticate LAN sync peer [address=%s] failed: %s", peerAddress, err)
		}
	}()
}

func (manager *Manager) newPeerHTTPClient() *http.Client {
	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			Certificates:       []tls.Certificate{manager.identity.certificate},
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
			NextProtos:         []string{"h2", "http/1.1"},
		},
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 15 * time.Second}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		IdleConnTimeout:       30 * time.Second,
	}
	return &http.Client{Transport: transport, Timeout: 5*time.Minute + 15*time.Second}
}

func (manager *Manager) clearPeerSession(current *peer) {
	current.mu.Lock()
	current.token = ""
	current.tokenExpires = time.Time{}
	if nil != current.client {
		current.client.CloseIdleConnections()
	}
	current.client = nil
	current.mu.Unlock()
}

func peerURL(address string, port int) string {
	return "https://" + net.JoinHostPort(address, fmt.Sprintf("%d", port))
}
