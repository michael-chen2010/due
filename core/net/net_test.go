package net_test

import (
	"fmt"
	"testing"

	"github.com/dobyte/due/v2/core/net"
)

func TestParseAddr(t *testing.T) {
	net.SetPublicIPResolver(customPublicIPResolver)
	listenAddr, exposeAddr, err := net.ParseAddr("0.0.0.0:0", true)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(listenAddr, exposeAddr)
}

func TestInternalIP(t *testing.T) {
	net.SetPrivateIPResolver(customPrivateIPResolver)
	ip, err := net.InternalIP()
	if err != nil {
		t.Fatal(err)
	}

	if ip != "192.168.1.1" {
		t.Fatalf("InternalIP() = %q, want %q", ip, "192.168.1.1")
	}
}

func TestExternalIP(t *testing.T) {
	net.SetPublicIPResolver(customPublicIPResolver)
	for range 100 {
		ip, err := net.ExternalIP()
		if err != nil {
			t.Fatal(err)
		}

		if ip != "1.1.1.1" {
			t.Fatalf("ExternalIP() = %q, want %q", ip, "1.1.1.1")
		}
	}
}

func TestPublicIP(t *testing.T) {
	net.SetPublicIPResolver(customPublicIPResolver)
	if ip, err := net.PublicIP(); err != nil {
		t.Fatal(err)
	} else if ip != "1.1.1.1" {
		t.Fatalf("PublicIP() = %q, want %q", ip, "1.1.1.1")
	}
}

func TestPrivateIP(t *testing.T) {
	net.SetPrivateIPResolver(customPrivateIPResolver)
	if ip, err := net.PrivateIP(); err != nil {
		t.Fatal(err)
	} else if ip != "192.168.1.1" {
		t.Fatalf("PrivateIP() = %q, want %q", ip, "192.168.1.1")
	}
}

func customPublicIPResolver() (string, error) {
	return "1.1.1.1", nil
}

func customPrivateIPResolver() (string, error) {
	return "192.168.1.1", nil
}
