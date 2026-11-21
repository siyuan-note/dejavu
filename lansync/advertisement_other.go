// DejaVu - Data snapshot and sync.
// Copyright (c) 2022-present, b3log.org
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !linux || android

package lansync

import (
	"net"

	"github.com/hashicorp/mdns"
)

func startMDNSAdvertisement(zone mdns.Zone, iface *net.Interface) (mdnsAdvertisement, error) {
	return mdns.NewServer(&mdns.Config{Zone: zone, Iface: iface})
}
