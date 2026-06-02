package client

type AgentSpec struct {
	Generation int64           `json:"generation"`
	Lifecycle  SpecLifecycle   `json:"lifecycle"`
	Disks      []AgentDiskSpec `json:"disks"`
}

type SpecLifecycle struct {
	DeletionRequestedAt *string `json:"deletion_requested_at"`
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
	ObservedGeneration int64               `json:"observed_generation"`
	Lifecycle          StatusLifecycle     `json:"lifecycle"`
	// omitempty: nil-slice сериализуется в `null`, что валит Zod-валидацию backend'а
	// (она ожидает массив или missing — null нелегитимен). Пустой массив тоже устроит,
	// но omitempty короче и совпадает с DTO-дефолтом backend'а (.default([])).
	Disks        []AgentDiskObserved `json:"disks,omitempty"`
	AgentVersion string              `json:"agent_version,omitempty"`
	UptimeSec    int64               `json:"uptime_sec,omitempty"`
}

type StatusLifecycle struct {
	ObservedState string `json:"observed_state"`
}

type AgentDiskObserved struct {
	ID            string  `json:"id"`
	ObservedState string  `json:"observed_state"`
	LastError     *string `json:"last_error"`
}

const (
	ObservedMounted   = "mounted"
	ObservedUnmounted = "unmounted"
	ObservedError     = "error"

	LifecycleAlive          = "alive"
	LifecycleSyncing        = "syncing"
	LifecycleSynced         = "synced"
	LifecycleDestroyingSelf = "destroying_self"
)

type RotateStorageKeysResponse struct {
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

type ProvisioningStatusRequest struct {
	Status    string  `json:"status"`
	Message   *string `json:"message,omitempty"`
	IPAddress *string `json:"ip_address,omitempty"`
	LogBase64 *string `json:"log_base64,omitempty"`
}
