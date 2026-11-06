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
	"crypto/tls"
	"encoding/base64"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// TestDiscoveredPeerWithoutIdentityProofReceivesNoSessionMetadata 校验客户端在验证服务端身份证明前
// 不发送 scope、设备名等会话元数据，重放发现标签的伪造对端只能收到 identity 请求。
func TestDiscoveredPeerWithoutIdentityProofReceivesNoSessionMetadata(t *testing.T) {
	validNonce := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	invalidProof := base64.RawURLEncoding.EncodeToString(make([]byte, 32))
	for _, test := range []struct {
		name string
		body string
	}{
		{name: "legacy identity without proof", body: `{"protocolVersion":1,"objectFormat":1}`},
		{name: "invalid identity proof", body: `{"protocolVersion":1,"objectFormat":1,"nonce":"` + validNonce + `","proof":"` + invalidProof + `"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := newTestManager(t, []byte("0123456789abcdef0123456789abcdef"), "main", nil)
			info := client.DiscoveryInfo()
			if nil == info || "" == info.TXT["scope"] {
				t.Fatal("client discovery info has no scope tag")
			}

			fakeIdentity, err := loadOrCreateIdentity(filepath.Join(t.TempDir(), "identity.json"))
			if nil != err {
				t.Fatal(err)
			}
			var sessionRequests atomic.Int32
			nonceHeader := make(chan string, 1)
			mux := http.NewServeMux()
			mux.HandleFunc("GET /peer-sync/v1/identity", func(writer http.ResponseWriter, request *http.Request) {
				select {
				case nonceHeader <- request.Header.Get(peerNonceHeader):
				default:
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(test.body))
			})
			mux.HandleFunc("POST /peer-sync/v1/session/open", func(writer http.ResponseWriter, request *http.Request) {
				sessionRequests.Add(1)
				http.Error(writer, "unexpected", http.StatusInternalServerError)
			})
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if nil != err {
				t.Fatal(err)
			}
			server := &http.Server{
				Handler: mux,
				TLSConfig: &tls.Config{
					Certificates: []tls.Certificate{fakeIdentity.certificate},
					ClientAuth:   tls.RequireAnyClientCert,
					MinVersion:   tls.VersionTLS13,
				},
			}
			go func() {
				_ = server.Serve(tls.NewListener(listener, server.TLSConfig))
			}()
			t.Cleanup(func() {
				_ = server.Close()
			})

			_, portText, err := net.SplitHostPort(listener.Addr().String())
			if nil != err {
				t.Fatal(err)
			}
			port, err := strconv.Atoi(portText)
			if nil != err {
				t.Fatal(err)
			}
			if !client.AddDiscoveredPeer("attacker._siyuan-sync._tcp.local.", "127.0.0.1", port, info.TXT) {
				t.Fatal("valid discovery tag was rejected")
			}

			select {
			case nonce := <-nonceHeader:
				if "" == nonce {
					t.Fatal("client did not request an identity proof")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("client did not request peer identity")
			}
			time.Sleep(200 * time.Millisecond)
			if got := sessionRequests.Load(); 0 != got {
				t.Fatalf("peer without a valid identity proof received %d session/open requests", got)
			}
			if got := client.ConnectedPeerCount(); 0 != got {
				t.Fatalf("unauthenticated peer was counted as connected: %d", got)
			}
		})
	}
}
