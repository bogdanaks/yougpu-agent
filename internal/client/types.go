package client

// AgentSpec — GET /agent/spec response. Mirror of AgentSpecResponseSchema
// in yougpu-backend/src/modules/instances/dto/agent-spec.dto.ts.
type AgentSpec struct {
	Generation int64           `json:"generation"`
	Lifecycle  SpecLifecycle   `json:"lifecycle"`
	Disks      []AgentDiskSpec `json:"disks"`
}

type SpecLifecycle struct {
	// nil ⇔ no terminate; non-nil RFC3339 timestamp ⇔ agent must graceful-sync.
	DeletionRequestedAt *string `json:"deletion_requested_at"`
}

type AgentDiskSpec struct {
	ID           string `json:"id"`
	DesiredState string `json:"desired_state"` // "mounted" | "unmounted"
	Bucket       string `json:"bucket"`
	S3Path       string `json:"s3_path"`
	MountPath    string `json:"mount_path"`
}

const (
	DesiredMounted   = "mounted"
	DesiredUnmounted = "unmounted"
)

// AgentStatus — POST /agent/status request body. Mirror of AgentStatusRequestSchema.
type AgentStatus struct {
	ObservedGeneration int64               `json:"observed_generation"`
	Lifecycle          StatusLifecycle     `json:"lifecycle"`
	Disks              []AgentDiskObserved `json:"disks"`
	AgentVersion       string              `json:"agent_version,omitempty"`
	UptimeSec          int64               `json:"uptime_sec,omitempty"`
}

type StatusLifecycle struct {
	ObservedState string `json:"observed_state"` // alive | syncing | synced | destroying_self
}

type AgentDiskObserved struct {
	ID            string  `json:"id"`
	ObservedState string  `json:"observed_state"` // mounted | unmounted | error
	LastError     *string `json:"last_error"`
}

const (
	ObservedMounted   = "mounted"
	ObservedUnmounted = "unmounted"
	ObservedError     = "error"

	LifecycleAlive           = "alive"
	LifecycleSyncing         = "syncing"
	LifecycleSynced          = "synced"
	LifecycleDestroyingSelf  = "destroying_self"
)

// RotateStorageKeysResponse — POST /agent/rotate-storage-keys response payload.
// Backend wraps it in the {status,code,data,meta} envelope via TransformInterceptor;
// the Client unwraps the envelope before decoding into this struct.
type RotateStorageKeysResponse struct {
	Endpoint     string `json:"endpoint"`
	AccessKey    string `json:"accessKey"`
	SecretKey    string `json:"secretKey"`
	SessionToken string `json:"sessionToken"`
}

// ProvisioningStatusRequest — POST /agent/provisioning-status body.
// Mirror of InstanceCallbackSchema.
type ProvisioningStatusRequest struct {
	Status    string  `json:"status"` // "success" | "error"
	Message   *string `json:"message,omitempty"`
	IPAddress *string `json:"ip_address,omitempty"`
	LogBase64 *string `json:"log_base64,omitempty"`
}
