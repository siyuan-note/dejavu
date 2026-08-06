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
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/siyuan-note/logging"
	"golang.org/x/net/http2"
)

func (manager *Manager) startServer() (err error) {
	listener, err := net.Listen("tcp", ":0")
	if nil != err {
		return err
	}
	tlsListener := tls.NewListener(listener, manager.tlsServerConfig())
	mux := http.NewServeMux()
	mux.HandleFunc("GET /peer-sync/v1/identity", manager.handleIdentity)
	mux.HandleFunc("POST /peer-sync/v1/session/open", manager.handleSessionOpen)
	mux.HandleFunc("POST /peer-sync/v1/chunks/has", manager.authorize(manager.handleHasChunks))
	mux.HandleFunc("GET /peer-sync/v1/chunks/{id}", manager.authorize(manager.handleDownloadChunk))
	mux.HandleFunc("POST /peer-sync/v1/commits/hint", manager.authorize(manager.handleCommitHint))
	server := &http.Server{
		Handler:           manager.privateNetworkOnly(mux),
		ReadTimeout:       15 * time.Second,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      5 * time.Minute,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	if err = http2.ConfigureServer(server, &http2.Server{MaxConcurrentStreams: uint32(manager.config.MaxConcurrentReqs)}); nil != err {
		_ = listener.Close()
		return err
	}
	manager.listener = listener
	manager.httpServer = server
	go func() {
		if serveErr := server.Serve(tlsListener); nil != serveErr && http.ErrServerClosed != serveErr {
			logging.LogWarnf("LAN sync server stopped unexpectedly: %s", serveErr)
		}
	}()
	return
}

func (manager *Manager) privateNetworkOnly(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		host, _, err := net.SplitHostPort(request.RemoteAddr)
		if nil != err {
			http.Error(writer, "forbidden", http.StatusForbidden)
			return
		}
		if index := strings.LastIndex(host, "%"); 0 < index {
			host = host[:index]
		}
		ip := net.ParseIP(host)
		if nil == ip || !(ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast()) {
			http.Error(writer, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (manager *Manager) handleIdentity(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]interface{}{
		"protocolVersion": ProtocolVersion,
		"objectFormat":    ObjectFormatVersion,
	})
}

func (manager *Manager) handleSessionOpen(writer http.ResponseWriter, request *http.Request) {
	if nil == request.TLS || 1 != len(request.TLS.PeerCertificates) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	body := &sessionOpenRequest{}
	if err := decodeJSONBody(writer, request, body, 16*1024); nil != err {
		return
	}
	if ProtocolVersion != body.ProtocolVersion || ObjectFormatVersion != body.ObjectFormat || manager.identity.id == body.DeviceID {
		http.Error(writer, "incompatible", http.StatusConflict)
		return
	}
	clientDeviceID, err := deviceIDFromCertificate(request.TLS.PeerCertificates[0])
	if nil != err || !hmac.Equal([]byte(clientDeviceID), []byte(body.DeviceID)) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	requestScope, err := base64.RawURLEncoding.DecodeString(body.Scope)
	if nil != err || !hmac.Equal(requestScope, manager.scopeID) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	clientNonce, err := base64.RawURLEncoding.DecodeString(body.Nonce)
	if nil != err || 32 != len(clientNonce) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	clientCertHash := sha256.Sum256(request.TLS.PeerCertificates[0].Raw)
	expectedProof := calculateProof(manager.authKey, "client", manager.scopeID, clientNonce, nil, clientCertHash[:],
		manager.identity.certHash)
	providedProof, err := base64.RawURLEncoding.DecodeString(body.Proof)
	expectedProofBytes, _ := base64.RawURLEncoding.DecodeString(expectedProof)
	if nil != err || !hmac.Equal(providedProof, expectedProofBytes) {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}

	serverNonce := make([]byte, 32)
	if _, err = rand.Read(serverNonce); nil != err {
		http.Error(writer, "internal error", http.StatusInternalServerError)
		return
	}
	tokenBytes := make([]byte, 32)
	if _, err = rand.Read(tokenBytes); nil != err {
		http.Error(writer, "internal error", http.StatusInternalServerError)
		return
	}
	token := base64.RawURLEncoding.EncodeToString(tokenBytes)
	manager.sessionMu.Lock()
	manager.cleanupExpiredSessionsLocked(time.Now())
	if maxSessions <= len(manager.sessions) {
		manager.sessionMu.Unlock()
		http.Error(writer, "too many sessions", http.StatusTooManyRequests)
		return
	}
	manager.sessions[token] = &serverSession{
		peerID:   body.DeviceID,
		certHash: hex.EncodeToString(clientCertHash[:]),
		expires:  time.Now().Add(sessionLifetime),
		lastSeen: time.Now(),
	}
	manager.sessionMu.Unlock()
	responseProof := calculateProof(manager.authKey, "server", manager.scopeID, clientNonce, serverNonce, clientCertHash[:],
		manager.identity.certHash)
	writeJSON(writer, http.StatusOK, &sessionOpenResponse{
		ProtocolVersion: ProtocolVersion,
		ObjectFormat:    ObjectFormatVersion,
		Nonce:           base64.RawURLEncoding.EncodeToString(serverNonce),
		DeviceID:        manager.identity.id,
		DeviceName:      manager.config.DeviceName,
		DeviceOS:        manager.config.DeviceOS,
		AppVersion:      manager.config.AppVersion,
		Token:           token,
		Proof:           responseProof,
	})
}

func (manager *Manager) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if nil == request.TLS || 1 != len(request.TLS.PeerCertificates) {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		value := strings.TrimSpace(request.Header.Get("Authorization"))
		if !strings.HasPrefix(value, "PeerSession ") {
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		token := strings.TrimSpace(strings.TrimPrefix(value, "PeerSession "))
		clientCertHash := sha256.Sum256(request.TLS.PeerCertificates[0].Raw)
		manager.sessionMu.Lock()
		session := manager.sessions[token]
		if nil == session || time.Now().After(session.expires) || session.certHash != hex.EncodeToString(clientCertHash[:]) {
			delete(manager.sessions, token)
			manager.sessionMu.Unlock()
			http.Error(writer, "unauthorized", http.StatusUnauthorized)
			return
		}
		session.expires = time.Now().Add(sessionLifetime)
		session.lastSeen = time.Now()
		manager.sessionMu.Unlock()
		next(writer, request)
	}
}

func (manager *Manager) handleHasChunks(writer http.ResponseWriter, request *http.Request) {
	body := &hasChunksRequest{}
	if err := decodeJSONBody(writer, request, body, 256*1024); nil != err {
		return
	}
	if maxHasChunks < len(body.IDs) {
		http.Error(writer, "too many chunk IDs", http.StatusRequestEntityTooLarge)
		return
	}
	found := make([]string, 0, len(body.IDs))
	for _, id := range body.IDs {
		if !validObjectID(id) {
			continue
		}
		if info, err := os.Lstat(manager.chunkPath(id)); nil == err && info.Mode().IsRegular() && info.Size() <= maxChunkSize {
			found = append(found, id)
		}
	}
	writeJSON(writer, http.StatusOK, &hasChunksResponse{IDs: found})
}

func (manager *Manager) handleDownloadChunk(writer http.ResponseWriter, request *http.Request) {
	id := request.PathValue("id")
	if !validObjectID(id) {
		http.Error(writer, "invalid chunk ID", http.StatusBadRequest)
		return
	}
	path := manager.chunkPath(id)
	pathInfo, err := os.Lstat(path)
	if nil != err || !pathInfo.Mode().IsRegular() || maxChunkSize < pathInfo.Size() {
		if nil != err && !os.IsNotExist(err) {
			http.Error(writer, "read failed", http.StatusInternalServerError)
			return
		}
		http.NotFound(writer, request)
		return
	}
	file, err := os.Open(path)
	if nil != err {
		if os.IsNotExist(err) {
			http.NotFound(writer, request)
			return
		}
		http.Error(writer, "read failed", http.StatusInternalServerError)
		return
	}
	defer file.Close()
	info, err := file.Stat()
	if nil != err || !info.Mode().IsRegular() || maxChunkSize < info.Size() || !os.SameFile(pathInfo, info) {
		http.NotFound(writer, request)
		return
	}
	writer.Header().Set("Content-Type", "application/octet-stream")
	writer.Header().Set("Cache-Control", "private, immutable")
	writer.Header().Set("ETag", `"`+id+`"`)
	writer.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	_, _ = io.Copy(writer, file)
}

func (manager *Manager) handleCommitHint(writer http.ResponseWriter, request *http.Request) {
	body := &commitHintRequest{}
	if err := decodeJSONBody(writer, request, body, 4*1024); nil != err {
		return
	}
	if !validObjectID(body.LatestID) {
		http.Error(writer, "invalid latest ID", http.StatusBadRequest)
		return
	}
	if nil != manager.config.OnCommitHint {
		go manager.config.OnCommitHint(body.LatestID)
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (manager *Manager) chunkPath(id string) string {
	return filepath.Join(manager.config.RepoPath, "objects", id[:2], id[2:])
}

func decodeJSONBody(writer http.ResponseWriter, request *http.Request, ret interface{}, maxBytes int64) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(ret); nil != err {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return err
	}
	if err := decoder.Decode(&struct{}{}); io.EOF != err {
		http.Error(writer, "invalid request", http.StatusBadRequest)
		return errors.New("request contains trailing JSON data")
	}
	return nil
}

func writeJSON(writer http.ResponseWriter, status int, value interface{}) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
