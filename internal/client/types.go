package client

import "time"

type AgentSpec struct {
	Generation int64               `json:"generation"`
	Lifecycle  SpecLifecycle       `json:"lifecycle"`
	Container  *AgentContainerSpec `json:"container"`
	Firewall   *AgentFirewallSpec  `json:"firewall"`
	Tunnel     *AgentTunnelSpec    `json:"tunnel"`
	Disks      []AgentDiskSpec     `json:"disks"`
}

type AgentFirewallSpec struct {
	Ports []FirewallPort `json:"ports"`
}

type FirewallPort struct {
	Port     int    `json:"port"`
	Protocol string `json:"protocol"`
}

type AgentTunnelSpec struct {
	Slug     string        `json:"slug"`
	FrpsAddr string        `json:"frps_addr"`
	FrpToken string        `json:"frp_token"`
	Proxies  []TunnelProxy `json:"proxies"`
}

type TunnelProxy struct {
	Subdomain string `json:"subdomain"`
	LocalPort int    `json:"local_port"`
}

type SpecLifecycle struct {
	DeletionRequestedAt *string `json:"deletion_requested_at"`
}

type AgentContainerSpec struct {
	Image      string            `json:"image"`
	RunCommand *string           `json:"run_command"`
	Env        map[string]string `json:"env"`
	Volumes    []ContainerVolume `json:"volumes"`
	ShmSizeGB  *float64          `json:"shm_size_gb"`
	GPU        bool              `json:"gpu"`
}

type ContainerVolume struct {
	Host      string `json:"host"`
	Container string `json:"container"`
}

type AgentDiskSpec struct {
	ID           string `json:"id"`
	DesiredState string `json:"desired_state"`
	Bucket       string `json:"bucket"`
	S3Path       string `json:"s3_path"`
	MountPath    string `json:"mount_path"`
}

const (
	DesiredMounted   = "mounted"
	DesiredUnmounted = "unmounted"
)

type AgentStatus struct {
	ObservedGeneration int64           `json:"observed_generation"`
	Lifecycle          StatusLifecycle `json:"lifecycle"`
	// omitempty: nil-slice сериализуется в `null`, что валит Zod-валидацию backend'а
	// (она ожидает массив или missing — null нелегитимен). Пустой массив тоже устроит,
	// но omitempty короче и совпадает с DTO-дефолтом backend'а (.default([])).
	Disks        []AgentDiskObserved     `json:"disks,omitempty"`
	Container    *AgentContainerObserved `json:"container,omitempty"`
	Firewall     *AgentFirewallObserved  `json:"firewall,omitempty"`
	Setup        *AgentSetupObserved     `json:"setup,omitempty"`
	AgentVersion string                  `json:"agent_version,omitempty"`
	UptimeSec    int64                   `json:"uptime_sec,omitempty"`
}

type StatusLifecycle struct {
	ObservedState string `json:"observed_state"`
}

type AgentDiskObserved struct {
	ID            string  `json:"id"`
	ObservedState string  `json:"observed_state"`
	LastError     *string `json:"last_error"`
}

type AgentContainerObserved struct {
	ObservedState string  `json:"observed_state"`
	Progress      *int    `json:"progress,omitempty"`
	Detail        *string `json:"detail,omitempty"`
	SpecHash      string  `json:"spec_hash,omitempty"`
	LastError     *string `json:"last_error"`
}

type AgentFirewallObserved struct {
	ObservedState string  `json:"observed_state"`
	LastError     *string `json:"last_error"`
}

type AgentSetupObserved struct {
	ObservedState string  `json:"observed_state"`
	Progress      *int    `json:"progress,omitempty"`
	Detail        *string `json:"detail,omitempty"`
	LastError     *string `json:"last_error,omitempty"`
	LastLog       *string `json:"last_log,omitempty"`
}

const (
	ObservedMounted   = "mounted"
	ObservedUnmounted = "unmounted"
	ObservedError     = "error"

	ContainerPulling  = "pulling"
	ContainerStarting = "starting"
	ContainerRunning  = "running"
	ContainerAbsent   = "absent"
	ContainerError    = "error"

	FirewallApplied = "applied"
	FirewallError   = "error"

	SetupInstallingBase    = "installing_base"
	SetupInstallingDocker  = "installing_docker"
	SetupConfiguringGPU    = "configuring_gpu"
	SetupInstallingStorage = "installing_storage"
	SetupReady             = "ready"
	SetupError             = "error"

	LifecycleAlive          = "alive"
	LifecycleSyncing        = "syncing"
	LifecycleSynced         = "synced"
	LifecycleDestroyingSelf = "destroying_self"
)

type StorageCredentials struct {
	Endpoint     string    `json:"endpoint"`
	AccessKey    string    `json:"accessKey"`
	SecretKey    string    `json:"secretKey"`
	SessionToken string    `json:"sessionToken"`
	ExpiresAt    time.Time `json:"expiresAt"`
	CredentialID string    `json:"credentialId"`
}

type ProvisioningStatusRequest struct {
	Status    string  `json:"status"`
	Message   *string `json:"message,omitempty"`
	IPAddress *string `json:"ip_address,omitempty"`
	LogBase64 *string `json:"log_base64,omitempty"`
}
