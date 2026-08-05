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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/88250/gulu"
)

type identityFile struct {
	PrivateKey string `json:"privateKey"`
}

type identity struct {
	id          string
	certificate tls.Certificate
	certHash    []byte
}

func deviceIDFromCertificate(certificate *x509.Certificate) (string, error) {
	publicKey, ok := certificate.PublicKey.(ed25519.PublicKey)
	if !ok || ed25519.PublicKeySize != len(publicKey) {
		return "", errors.New("invalid LAN sync certificate public key")
	}
	publicHash := sha256.Sum256(publicKey)
	return hex.EncodeToString(publicHash[:16]), nil
}

func loadOrCreateIdentity(path string) (ret *identity, err error) {
	var privateKey ed25519.PrivateKey
	data, readErr := os.ReadFile(path)
	if nil == readErr {
		stored := &identityFile{}
		if err = json.Unmarshal(data, stored); nil != err {
			return nil, err
		}
		decoded, decodeErr := base64.StdEncoding.DecodeString(stored.PrivateKey)
		if nil != decodeErr || ed25519.PrivateKeySize != len(decoded) {
			return nil, errors.New("invalid LAN sync identity")
		}
		privateKey = ed25519.PrivateKey(decoded)
	} else if !os.IsNotExist(readErr) {
		return nil, readErr
	} else {
		_, privateKey, err = ed25519.GenerateKey(rand.Reader)
		if nil != err {
			return nil, err
		}
		stored := &identityFile{PrivateKey: base64.StdEncoding.EncodeToString(privateKey)}
		data, err = json.Marshal(stored)
		if nil != err {
			return nil, err
		}
		if err = os.MkdirAll(filepath.Dir(path), 0755); nil != err {
			return nil, err
		}
		if err = gulu.File.WriteFileSafer(path, data, 0600); nil != err {
			return nil, err
		}
	}

	publicKey := privateKey.Public().(ed25519.PublicKey)
	publicHash := sha256.Sum256(publicKey)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if nil != err {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "SiYuan LAN Sync " + hex.EncodeToString(publicHash[:8])},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
	}
	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if nil != err {
		return nil, err
	}
	certificate := tls.Certificate{Certificate: [][]byte{certificateDER}, PrivateKey: privateKey}
	certHash := sha256.Sum256(certificateDER)
	ret = &identity{
		id:          hex.EncodeToString(publicHash[:16]),
		certificate: certificate,
		certHash:    append([]byte(nil), certHash[:]...),
	}
	return
}
