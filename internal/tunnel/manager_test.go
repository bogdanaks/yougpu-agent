package tunnel

import (
	"testing"

	v1 "github.com/fatedier/frp/pkg/config/v1"

	"github.com/bogdanaks/yougpu-agent/internal/client"
)

func TestSplitAddr(t *testing.T) {
	host, port, err := splitAddr("gw.example.com:7000")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if host != "gw.example.com" || port != 7000 {
		t.Fatalf("got %s:%d", host, port)
	}
	if _, _, err := splitAddr("no-port"); err == nil {
		t.Fatal("expected error for addr without port")
	}
}

func TestBuildConfig(t *testing.T) {
	spec := &client.AgentTunnelSpec{
		Slug:     "k7m2p9qx",
		FrpsAddr: "gw.example.com:7000",
		FrpToken: "secret-token",
		Proxies: []client.TunnelProxy{
			{Subdomain: "k7m2p9qx-jupyter", LocalPort: 8888},
			{Subdomain: "k7m2p9qx-comfyui", LocalPort: 8188},
		},
	}

	common, proxies, err := buildConfig(spec)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if common.ServerAddr != "gw.example.com" || common.ServerPort != 7000 {
		t.Fatalf("server %s:%d", common.ServerAddr, common.ServerPort)
	}
	if common.Metadatas["slug"] != "k7m2p9qx" || common.Metadatas["token"] != "secret-token" {
		t.Fatalf("metadatas %v", common.Metadatas)
	}
	if len(proxies) != 2 {
		t.Fatalf("want 2 proxies, got %d", len(proxies))
	}

	hp, ok := proxies[0].(*v1.HTTPProxyConfig)
	if !ok {
		t.Fatalf("proxy 0 is %T, want *v1.HTTPProxyConfig", proxies[0])
	}
	if hp.Type != "http" || hp.SubDomain != "k7m2p9qx-jupyter" || hp.LocalPort != 8888 {
		t.Fatalf("proxy 0 = %s/%s:%d", hp.Type, hp.SubDomain, hp.LocalPort)
	}
}

func TestSpecHash(t *testing.T) {
	a := &client.AgentTunnelSpec{FrpsAddr: "gw:7000", FrpToken: "t", Proxies: []client.TunnelProxy{{Subdomain: "s", LocalPort: 80}}}
	b := &client.AgentTunnelSpec{FrpsAddr: "gw:7000", FrpToken: "t", Proxies: []client.TunnelProxy{{Subdomain: "s", LocalPort: 81}}}

	if specHash(a) != specHash(a) {
		t.Fatal("hash must be stable for the same spec")
	}
	if specHash(a) == specHash(b) {
		t.Fatal("hash must differ when a proxy port differs")
	}
}
