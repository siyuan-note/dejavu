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
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"io"
	"strconv"
	"strings"
	"time"

	"golang.org/x/crypto/hkdf"
)

const (
	ProtocolVersion     = 1
	ObjectFormatVersion = 1
	ServiceName         = "_siyuan-sync._tcp"

	discoveryWindow = 5 * time.Minute
	sessionLifetime = 10 * time.Minute
	maxHasChunks    = 4096
	maxChunkSize    = 128 * 1024 * 1024
	maxSessions     = 4096
)

type sessionOpenRequest struct {
	ProtocolVersion int    `json:"protocolVersion"`
	ObjectFormat    int    `json:"objectFormat"`
	Scope           string `json:"scope"`
	Nonce           string `json:"nonce"`
	DeviceID        string `json:"deviceID"`
	DeviceName      string `json:"deviceName"`
	DeviceOS        string `json:"deviceOS"`
	AppVersion      string `json:"appVersion"`
	Proof           string `json:"proof"`
}

type sessionOpenResponse struct {
	ProtocolVersion int    `json:"protocolVersion"`
	ObjectFormat    int    `json:"objectFormat"`
	Nonce           string `json:"nonce"`
	DeviceID        string `json:"deviceID"`
	DeviceName      string `json:"deviceName"`
	DeviceOS        string `json:"deviceOS"`
	AppVersion      string `json:"appVersion"`
	Token           string `json:"token"`
	Proof           string `json:"proof"`
}

type hasChunksRequest struct {
	IDs []string `json:"ids"`
}

type hasChunksResponse struct {
	IDs []string `json:"ids"`
}

type commitHintRequest struct {
	LatestID string `json:"latestID"`
}

func deriveKey(master []byte, purpose string) []byte {
	reader := hkdf.New(sha256.New, master, nil, []byte(purpose))
	ret := make([]byte, 32)
	if _, err := io.ReadFull(reader, ret); nil != err {
		panic("derive LAN sync key failed: " + err.Error())
	}
	return ret
}

func calculateScopeID(discoveryKey []byte, scope string) []byte {
	mac := hmac.New(sha256.New, discoveryKey)
	mac.Write([]byte(scope))
	return mac.Sum(nil)
}

func calculateDiscoveryTag(discoveryKey, scopeID []byte, now time.Time) string {
	window := now.Unix() / int64(discoveryWindow/time.Second)
	mac := hmac.New(sha256.New, discoveryKey)
	mac.Write(scopeID)
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], uint64(window))
	mac.Write(value[:])
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil)[:16])
}

func calculateProof(authKey []byte, role string, scopeID, clientNonce, serverNonce, clientCertHash,
	serverCertHash []byte) string {
	mac := hmac.New(sha256.New, authKey)
	writeProofField(mac, []byte("siyuan-lan-sync-proof-v1"))
	writeProofField(mac, []byte(role))
	writeProofField(mac, []byte(strconv.Itoa(ProtocolVersion)))
	writeProofField(mac, []byte(strconv.Itoa(ObjectFormatVersion)))
	writeProofField(mac, scopeID)
	writeProofField(mac, clientNonce)
	writeProofField(mac, serverNonce)
	writeProofField(mac, clientCertHash)
	writeProofField(mac, serverCertHash)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func writeProofField(writer io.Writer, value []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = writer.Write(length[:])
	_, _ = writer.Write(value)
}

func validObjectID(id string) bool {
	if 40 != len(id) {
		return false
	}
	decoded, err := hex.DecodeString(id)
	return nil == err && 20 == len(decoded) && id == strings.ToLower(id)
}
