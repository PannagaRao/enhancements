# KEP-6276: Workload-Aware Scheduling for Deployments

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [Deployment Integration - API Usage Examples](#deployment-integration---api-usage-examples)
    - [Example 1: Gang scheduling with zone topology and atomic disruption](#example-1-gang-scheduling-with-zone-topology-and-atomic-disruption)
    - [Example 2: Gang scheduling with Recreate strategy](#example-2-gang-scheduling-with-recreate-strategy)
    - [Example 3: Gang with template-backed ResourceClaims](#example-3-gang-with-template-backed-resourceclaims)
  - [User Stories](#user-stories)
  - [Notes/Constraints/Caveats](#notesconstraintscaveats)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [API Changes](#api-changes)
  - [Feature Gate and RBAC](#feature-gate-and-rbac)
  - [Controller Changes](#controller-changes)
    - [Creation Ordering](#creation-ordering)
    - [workloadbuilder Integration](#workloadbuilder-integration)
    - [EqualIgnoreHash](#equalignorehash)
    - [Scaling and HPA](#scaling-and-hpa)
    - [Garbage Collection](#garbage-collection)
  - [Mutability and Validation](#mutability-and-validation)
  - [Test Plan](#test-plan)
    - [Unit Tests](#unit-tests)
    - [Integration Tests](#integration-tests)
    - [E2E Tests](#e2e-tests)
  - [Graduation Criteria](#graduation-criteria)
    - [Alpha](#alpha)
    - [Beta](#beta)
    - [GA](#ga)
  - [Upgrade / Downgrade Strategy](#upgrade--downgrade-strategy)
  - [Version Skew Strategy](#version-skew-strategy)
- [Production Readiness Review Questionnaire](#production-readiness-review-questionnaire)
  - [Feature Enablement and Rollback](#feature-enablement-and-rollback)
  - [Rollout, Upgrade and Rollback Planning](#rollout-upgrade-and-rollback-planning)
  - [Monitoring Requirements](#monitoring-requirements)
  - [Dependencies](#dependencies)
  - [Scalability](#scalability)
  - [Troubleshooting](#troubleshooting)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
<!-- /toc -->

## Summary

This KEP integrates the Workload-aware Scheduling (WAS) APIs (`Workload` and `PodGroup`) into
`apps/v1` Deployment by adding a user-facing `spec.scheduling` field, allowing users to express
scheduling intent such as gang scheduling and topology co-location for long-running replicated
services. The Deployment controller compiles `spec.scheduling` into one `Workload` per Deployment
and one `PodGroup` per ReplicaSet via the shared `workloadbuilder` library ([KEP-6089]), adapting
the controller-as-compiler pattern established by the Job + WAS integration ([KEP-5547]) to the
Deployment/ReplicaSet rollout, scaling, and revision-history lifecycle.

## Motivation

Long-running inference services - multi-GPU model servers, disaggregated prefill/decode
pipelines - commonly run as `apps/v1.Deployment` objects and require all replicas co-located
within the same topology domain or placed atomically to avoid wasting accelerator capacity on
partially placed groups.

Today the only path to gang-schedule or topology-schedule a Deployment is to manually create a
`PodGroup` and inject `pod.spec.schedulingGroup.podGroupName` into the pod template. This approach
is fragile: a pod created before its referenced PodGroup exists hangs silently in Pending with no
event or error. It also places the entire burden of naming, ownership, garbage collection, and
scale-time reconciliation on the user - none of which composes cleanly with rolling updates,
revision history, or HPA-driven scaling. Alternatively, users can turn to external solutions like
Volcano, Kueue, KAI or Coscheduling plugin.

### Goals

- Add a user-facing `spec.scheduling` (`DeploymentSchedulingConfiguration`) field to `apps/v1`
  Deployment, embedding the `scheduling.k8s.io/v1alpha3` building blocks (`schedulingPolicy`,
  `schedulingConstraints`, `disruptionMode`, `resourceClaims`) so users can express explicit
  scheduling intent for long-running replicated services.
- Compile `spec.scheduling` into one `Workload` per Deployment and one `PodGroup` per ReplicaSet
  via the shared `workloadbuilder` library ([KEP-6089]), adapting the controller-as-compiler
  pattern from the Job integration ([KEP-5547]).
- Derive gang `minCount` from `spec.replicas` (controller-set, not user-set), ensuring the entire
  replica set is atomically schedulable.
- Support both `RollingUpdate` and `Recreate` strategies with gang scheduling, rejecting at
  admission configurations that are structurally guaranteed to deadlock (e.g., `RollingUpdate`
  where resolved `maxSurge < replicas`).
- Support horizontal scaling and HPA natively through the Deployment `/scale` subresource.
- Ensure proper ordering of `Workload` → `PodGroup` → ReplicaSet creation, with deterministic
  naming and owner-reference lifecycle: `Workload` owned by the Deployment, `PodGroup` owned by
  the ReplicaSet.
- Make scheduling failures observable through standard Deployment conditions (`Available`,
  `Progressing`) rather than silent partial placement.

### Non-Goals

- Automatic in-tree recovery or rescheduling of a replacement pod stuck on a saturated topology
  domain - left to out-of-tree queue managers.
- Incremental rolling updates with gang scheduling - only blue-green style rollouts
  (`maxSurge >= replicas`) are supported. Smaller surge values deadlock permanently and are
  rejected at admission.
- Supporting mutable `spec.scheduling` post-creation (toggle on/off, flip gang↔basic, or change
  topology constraints); scheduling configuration is immutable for Alpha.
- Mutable `minCount` for elastic gang scaling - unlike Job, Deployment derives `minCount` from
  `spec.replicas`; elastic semantics are a Beta follow-up.
- Multi-level or nested composite (`CompositePodGroup`) structures; this KEP covers single-level
  Deployment → ReplicaSet workloads only.
- Exclusive access to DRA claims - any pod on the same node can reference a PodGroup's claim by
  name and share the device. Claim isolation is a DRA-layer property; this KEP does not add access
  control beyond what DRA provides.

## Proposal

This proposal builds on the recently introduced Workload-aware Scheduling enhancements. We assume
the reader is acquainted with the following KEPs:

- [KEP-4671]: Gang Scheduling.
- [KEP-5710]: Workload-aware preemption.
- [KEP-5732]: Topology-aware workload scheduling.
- [KEP-6089]: WAS Controller APIs.

The Deployment controller is extended to compile the user's scheduling intent into a single
`Workload` (created once per Deployment) and one `PodGroup` per ReplicaSet. Each PodGroup is
stamped from the Workload's `PodGroupTemplate`, carrying the scheduling policy, topology
constraints, and resource claims into a per-revision runtime context. The intent is expressed
through a new `spec.scheduling` field.

The key design principles:

- One `Workload` per Deployment serves as the shared scheduling template. Each ReplicaSet revision
  gets its own `PodGroup` stamped from that template, with an independent scheduling context
  (topology domain, gang quorum).
- The scheduling policy comes from the user's `spec.scheduling`, not from the Deployment's
  strategy. When `spec.scheduling` is omitted, no scheduling objects are created.
- Gang `minCount` is always derived from `spec.replicas` - user-set values are rejected.
- All `spec.scheduling` fields are immutable after creation.
- The `Workload` is owned by the Deployment for its entire lifetime. The `PodGroup` is
  bootstrap-owned by the Deployment, then reowned to the ReplicaSet once it is created. This
  guarantees no orphans if the controller crashes mid-sequence.

### Deployment Integration - API Usage Examples

#### Example 1: Gang scheduling with zone topology and atomic disruption

A multi-GPU inference service whose 3 replicas must schedule together, co-locate within the same
availability zone, and be disrupted atomically:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: inference-server
  namespace: ml-serving
spec:
  replicas: 3
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 3
      maxUnavailable: 0
  scheduling:
    schedulingPolicy:
      gang: {}
    schedulingConstraints:
      topology:
        - key: "topology.kubernetes.io/zone"
    disruptionMode:
      all: {}
  selector:
    matchLabels:
      app: inference-server
  template:
    metadata:
      labels:
        app: inference-server
    spec:
      containers:
      - name: server
        image: inference-server:v1
        resources:
          limits:
            nvidia.com/gpu: 1
```

The Deployment controller compiles this intent into a `Workload` owned by the Deployment and a
`PodGroup` for the resulting ReplicaSet. The `PodGroup` is initially bootstrap-owned by the
Deployment, then reowned to the ReplicaSet once it is created with a valid UID. The `Workload`
remains Deployment-owned and is reused across rollouts:

```yaml
apiVersion: scheduling.k8s.io/v1alpha3
kind: Workload
metadata:
  name: inference-server
  namespace: ml-serving
  ownerReferences:
  - apiVersion: apps/v1
    kind: Deployment
    name: inference-server
    uid: <deployment-uid>
    controller: false
    blockOwnerDeletion: false
spec:
  controllerRef:
    apiVersion: apps/v1
    kind: Deployment
    name: inference-server
  podGroupTemplates:
  - name: inference-server
    schedulingPolicy:
      gang:
        minCount: 3
    schedulingConstraints:
      topology:
        - key: "topology.kubernetes.io/zone"
    disruptionMode:
      all: {}
---
apiVersion: scheduling.k8s.io/v1alpha3
kind: PodGroup
metadata:
  name: inference-server-<podTemplateHash>
  namespace: ml-serving
  ownerReferences:
  - apiVersion: apps/v1
    kind: ReplicaSet
    name: inference-server-<podTemplateHash>
    uid: <rs-uid>
    controller: false
    blockOwnerDeletion: false
spec:
  schedulingPolicy:
    gang:
      minCount: 3
  schedulingConstraints:
    topology:
      - key: "topology.kubernetes.io/zone"
  disruptionMode:
    all: {}
```

#### Example 2: Gang scheduling with Recreate strategy

A simpler configuration using `Recreate` - no `maxSurge` constraint required:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prefill-workers
spec:
  replicas: 4
  strategy:
    type: Recreate
  scheduling:
    schedulingPolicy:
      gang: {}
  selector:
    matchLabels:
      app: prefill-workers
  template:
    metadata:
      labels:
        app: prefill-workers
    spec:
      containers:
      - name: worker
        image: prefill:v2
        resources:
          limits:
            nvidia.com/gpu: 2
```

#### Example 3: Gang with template-backed ResourceClaims

A gang Deployment that requests a shared DRA device allocated once per PodGroup:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: model-server
spec:
  replicas: 3
  strategy:
    type: Recreate
  scheduling:
    schedulingPolicy:
      gang: {}
    resourceClaims:
    - name: gpu-pool
      resourceClaimTemplateName: gpu-template
  selector:
    matchLabels:
      app: model-server
  template:
    metadata:
      labels:
        app: model-server
    spec:
      containers:
      - name: server
        image: model-server:v1
        resources:
          claims:
          - name: gpu-pool
```

### User Stories

**Distributed inference server.** A platform team runs a tensor-parallel inference service as a
Deployment. All replicas must be scheduled together or not at all, because a partially placed set
wastes accelerator capacity without serving traffic. The team sets
`spec.scheduling.schedulingPolicy.gang: {}` and lets HPA scale the Deployment with load. Each
scale-up either places the full new replica count atomically or fails visibly - never partially.

**Rack-local worker pool.** A latency-sensitive service needs all its pods co-located within one
rack. The team adds a topology constraint on `topology.kubernetes.io/rack`. The scheduler places
the whole gang in a best-fit rack, and the Deployment surfaces a clear `Available=False` status if
no single rack can satisfy the request.

### Notes/Constraints/Caveats

- User-set `gang.minCount` is rejected. If `minCount > replicas`, the gang can never be satisfied
  and the Deployment stays Pending indefinitely. If `minCount < replicas`, true partial-gang
  semantics require multiple PodGroups per ReplicaSet, which is deferred to Beta.
- `RollingUpdate` with gang requires `maxSurge >= replicas`. Smaller surge values deadlock
  permanently and are rejected at admission.
- Template-backed ResourceClaims hold 2× device allocations during rollouts (current + previous
  revision retained by `revisionHistoryLimit`). Allocations are released only when the old
  ReplicaSet is pruned past the history limit.
- Named ResourceClaims pin all revisions to the same node (the node where the device is allocated).
  If the node lacks capacity for 2× the gang during a rollout, the update deadlocks.
- Topology binding is permanent per PodGroup - a replacement pod stuck on a full domain will not
  automatically reschedule to a different domain.
- At `replicas=0`, the controller leaves `minCount` unchanged (functionally harmless; possible Beta
  tidy-up).
- No informer/lister for Workload/PodGroup in Alpha (direct API calls); Beta follow-up.
- When `DRAWorkloadResourceClaims` gate is off, `spec.scheduling.resourceClaims` is stored on the
  Deployment but silently stripped from the PodGroup by the apiserver. Pods fall back to per-pod
  claims instead of shared PodGroup-level claims. Alpha gap - rejection deferred to Beta.

### Risks and Mitigations

- **Silent deadlock from misconfigured `maxSurge`.** A user sets `maxSurge < replicas` with gang,
  causing a permanent rollout stall. *Mitigation:* admission validation rejects this combination
  with an actionable error directing the user to raise `maxSurge` or switch to `Recreate`.

- **Orphaned scheduling objects on controller crash.** The controller crashes between creating the
  PodGroup and creating the ReplicaSet. *Mitigation:* bootstrap ownership to the Deployment
  guarantees GC; reown completes on the next sync via deterministic naming.

- **Named ResourceClaim deadlocks rollouts on tight nodes.** The claim pins all pods to one node;
  if it lacks capacity for 2× the gang, the rollout hangs. *Mitigation:* documented limitation;
  users should prefer template-backed claims or ensure node headroom.

- **Template-backed claims hold 2× devices during rollout.** Both revisions' PodGroups retain
  their claims until the old ReplicaSet is pruned. *Mitigation:* bounded by
  `revisionHistoryLimit`; documented trade-off.

- **Feature gate disabled after objects exist.** Scheduling objects were created while the gate was
  on, then the gate is turned off. *Mitigation:* the Workload is owned by the Deployment and the
  PodGroup is owned by the ReplicaSet. Standard GC cleans up the PodGroup when the RS is pruned
  and the Workload when the Deployment is deleted; existing pods continue running without
  scheduling semantics.

## Design Details

### API Changes

A new optional field is added to `DeploymentSpec` in both the internal (`pkg/apis/apps`) and
external (`apps/v1`) types:

```go
// Scheduling, if set, opts this Deployment into Workload-Aware Scheduling.
// The controller compiles one Workload per Deployment and one PodGroup per
// ReplicaSet with gang minCount == replicas.
//
// +featureGate=WorkloadWithDeployment
// +optional
// +k8s:ifDisabled(WorkloadWithDeployment)=+k8s:forbidden
// +k8s:optional
// +k8s:update=NoSet
// +k8s:update=NoUnset
Scheduling *DeploymentSchedulingConfiguration `json:"scheduling,omitempty"`
```

`DeploymentSchedulingConfiguration` mirrors `batch/v1.JobSchedulingConfiguration`, reusing the
`scheduling.k8s.io/v1alpha3` building-block types directly:

```go
type DeploymentSchedulingConfiguration struct {
    // SchedulingPolicy selects the gang marker. The user sets gang: {};
    // the controller derives minCount. A user-supplied minCount is rejected.
    // +optional
    // +k8s:optional
    // +k8s:update=NoSet
    // +k8s:update=NoUnset
    SchedulingPolicy *WorkloadPodGroupSchedulingPolicy

    // SchedulingConstraints carries topology placement rules.
    // +optional
    // +k8s:optional
    // +k8s:immutable
    SchedulingConstraints *WorkloadPodGroupSchedulingConstraints

    // DisruptionMode (single | all) is passed through to the PodGroup
    // and consumed by scheduler preemption logic.
    // +optional
    // +k8s:optional
    // +k8s:immutable
    DisruptionMode *WorkloadPodGroupDisruptionMode

    // ResourceClaims declares DRA ResourceClaims shared across all pods
    // of the gang (allocated once to the PodGroup, not per-pod). Max 4
    // entries. Immutable after creation.
    // +optional
    // +listType=map
    // +listMapKey=name
    // +k8s:maxItems=4
    // +k8s:immutable
    ResourceClaims []WorkloadPodGroupResourceClaim
}
```

### Feature Gate and RBAC

The `Scheduling` field is gated by `WorkloadWithDeployment` (Alpha, default off). Standard alpha
field-gating semantics apply: when the gate is disabled, the API server clears `spec.scheduling`
on create and ignores it on update (preserving an already-set value on the stored object). The
gate depends on `GenericWorkload` being enabled.

The `deployment-controller` ClusterRole gains `get`, `list`, `watch`, `create`, `update`, `patch`,
and `delete` on `scheduling.k8s.io/workloads` and `scheduling.k8s.io/podgroups`.

### Controller Changes

#### Creation Ordering

For each new ReplicaSet, strictly before the ReplicaSet is created, the controller performs the
following steps (gated on `WorkloadWithDeployment` and `d.Spec.Scheduling != nil`):

1. **Deterministic naming.** The Workload is named `<deployment.Name>` (one per Deployment).
   The PodGroup is named `<deployment.Name>-<podTemplateHash>` (one per ReplicaSet revision).
   The pod-template hash makes PodGroup naming stable across controller restarts and identical
   for the same revision.
2. **Template injection.** Set `pod.spec.schedulingGroup.podGroupName` on the ReplicaSet pod
   template so every pod the ReplicaSet creates references the group.
3. **Workload creation.** Call `ensureWorkloadForDeployment` (get-or-create) to instantiate a
   `Workload` owned by the Deployment. If the Workload already exists (from a prior revision),
   its `podGroupTemplate.minCount` is patched to match the current `spec.replicas`.
4. **PodGroup creation.** Call `ensurePodGroupForRS` (get-or-create) to instantiate a `PodGroup`
   with `gang.minCount = spec.replicas`, owned by the Deployment. This must complete before the
   ReplicaSet exists, or pods would reference a nonexistent group.
5. **ReplicaSet creation.** Create the ReplicaSet.
6. **Ownership hand-off.** Once the ReplicaSet returns with a valid UID, issue a merge patch
   (`reownPodGroupToRS`) replacing the PodGroup's owner reference from the Deployment to the
   ReplicaSet (`controller: false`, `blockOwnerDeletion: false`). The Workload remains
   Deployment-owned and is not reowned.

#### workloadbuilder Integration

The compiler reuses the shared `workloadbuilder` library (the same one Job uses): it maps the
Deployment's `spec.scheduling` through a callback that forces `MinCount = replicas`, builds a
`Workload`, and derives the `PodGroup` from it.

#### EqualIgnoreHash

The controller injects `SchedulingGroup` into the ReplicaSet pod template, but it is absent from
the Deployment template. Without excluding this field from the template-equality check, every
reconcile would misdetect a template drift, driving endless collisionCount / ReplicaSet / PodGroup
churn. This exclusion is unconditional (not gated), because a stored ReplicaSet may carry the
field even after the gate is turned off.

#### Scaling and HPA

Scaling flows through the same `ensureWorkloadForDeployment` / `ensurePodGroupForRS` path. When an
operator or HPA patches `spec.replicas` via the `/scale` subresource, the sync loop patches both
the Workload's PodGroupTemplate `gang.minCount` and the active PodGroup's `gang.minCount` to
match the new replica count. The ReplicaSet controller then creates the new pods, and the
scheduler evaluates the gang permit atomically.

Decreasing `minCount` is immediately safe and never disturbs running pods; increasing it gates
only the newly created pods. A scale-up that cannot fully place binds zero new pods and leaves
the running set untouched, surfacing the shortfall through `Available=False`.

#### Garbage Collection

No explicit delete logic lives in the controller. GC is handled entirely by:
- The PodGroup's owner reference to the ReplicaSet - when a RS is pruned by
  `revisionHistoryLimit`, the PodGroup receives a `deletionTimestamp`.
- The Workload's owner reference to the Deployment - when the Deployment is deleted, the
  Workload is garbage-collected.
- The `scheduling.k8s.io/podgroup-protection` finalizer - the PodGroup is removed only after
  the last referencing pod drains.

### Mutability and Validation

`spec.scheduling` is validated in three complementary layers:

1. **Declarative validation (DV) on the building blocks** owns the structural rules and most of
   the immutability. Because the Deployment API embeds the versioned
   `scheduling.k8s.io/v1alpha3` building blocks directly, their DV markers apply unchanged.

2. **Hand-written Deployment validation** covers the cross-cutting rules DV cannot express:
   - **User-set `gang.minCount` is forbidden.** If the gang policy carries a non-nil `MinCount`,
     the request is rejected - the value is derived from `spec.replicas`.
   - **`RollingUpdate` with gang:** if resolved `maxSurge` is less than `replicas`, the request is
     rejected with an actionable message.
3. **`workloadbuilder` semantic validation** owns the consistency rules that must stay identical
   to what the controller compiles. Validation builds the same `WorkloadItem` tree the controller
   does and calls `NewBuilder(...).Validate()`, running the builder's allow-list checks. In-tree
   it is constructed with `BuildOptions{DisableDeclarativeValidation: true}` because the
   API server already ran DV on the versioned building blocks.

### Test Plan

#### Unit Tests

- Building the Workload and PodGroup: `minCount` equals replicas, topology constraints copied,
  owner reference set to Deployment, resourceClaims copied.
- ReplicaSet creation ordering: SchedulingGroup injected into pod template, Workload and PodGroup
  created before the ReplicaSet.
- Validation: user-set `minCount` rejected, `maxSurge < replicas` with gang rejected, immutability
  violations rejected, resourceClaims structural violations rejected.

#### Integration Tests

- Create a gang Deployment and verify a Workload and PodGroup with `minCount == replicas` exist,
  and the ReplicaSet template carries `schedulingGroup.podGroupName`.
- Scale up/down and verify `minCount` is patched on both the Workload template and the active
  PodGroup.
- Delete Deployment and verify the Workload is garbage-collected via its Deployment owner
  reference and the PodGroup via its ReplicaSet owner reference.

#### E2E Tests

- Gang Deployment (Recreate): all replicas bind atomically or none bind.
- Gang Deployment (RollingUpdate, `maxSurge >= replicas`): old and new gangs coexist; new gang
  surges to full size.
- Topology placement: gang lands in a single domain.
- DisruptionMode `single` vs `all`: preemption behavior differs as expected.
- ResourceClaims (template-backed): one claim per PodGroup, GC'd with PodGroup.
- ResourceClaims (named): shared across revisions, pinned to one node.
- Controller crash recovery: deterministic naming yields idempotent recovery, zero duplicates.
- Scale-to-zero-and-back: PodGroup persists, minCount unchanged, scale-up re-satisfies gang.
- Scaling up does not disturb existing running pods: only new pods are gated on the updated
  gang quorum.
- Single pod replacement: a deleted pod is replaced individually without re-gating the entire
  gang.

### Graduation Criteria

#### Alpha

- Feature implemented behind the `WorkloadWithDeployment` feature gate (default: disabled).
- Deployment controller creates one Workload per Deployment and one PodGroup per ReplicaSet
  when the gate is enabled and `spec.scheduling` is set.
- Gang scheduling with `minCount == replicas`, topology constraints, disruption mode, and
  resourceClaims all wired end-to-end.
- Admission validation rejects user-set `minCount`, `maxSurge < replicas` with gang, immutability
  violations, and resourceClaims structural violations.
- Unit and integration tests for the creation flow, validation, and GC.

#### Beta

- Promote `WorkloadWithDeployment` to enabled by default.
- Investigate scheduler-side failure reporting for stuck topology domains - dependent on
  sig-scheduling exposing standardized signals distinguishing terminal from transient
  unschedulability.
- Re-evaluate whether user-set `gang.minCount < replicas` (partial gangs) can be supported and
  what multi-PodGroup-per-ReplicaSet semantics would look like.
- Re-evaluate whether `RollingUpdate` with `maxSurge < replicas` can be supported for partial
  gangs or smaller quorum sizes.
- Re-evaluate wiring PodGroup informer/lister to replace direct API calls.
- Re-evaluate patching `minCount` to zero when `replicas=0`.
- Re-evaluate switching to `blockOwnerDeletion: true` on the RS owner reference to ensure
  ordered cleanup of scheduling objects.
- Re-evaluate Deployment-side admission rejection of `spec.scheduling.resourceClaims` when
  `DRAWorkloadResourceClaims` is off (currently a silent semantic downgrade).
- E2E test coverage for the full scenario matrix.

#### GA

TBD

### Upgrade / Downgrade Strategy

With the gate disabled or `spec.scheduling` unset, Deployments behave exactly as they do today -
no scheduling objects are created. On downgrade, PodGroups remain protected by their
`podgroup-protection` finalizers and clean up automatically when their parent ReplicaSet is
garbage-collected. The Workload cleans up when the Deployment is deleted. No stale pod-level
references remain, because a pod carries `schedulingGroup` only if it was created while the gate
was enabled.

### Version Skew Strategy

The feature requires `GenericWorkload` and the `scheduling.k8s.io` API versions to be active on
the API server. If the API server does not serve these resources, the controller's create/patch
calls fail and the Deployment sync retries with backoff - no scheduling objects are compiled until
the API server is upgraded. Pods created without `schedulingGroup` schedule normally through the
default path.

## Production Readiness Review Questionnaire

### Feature Enablement and Rollback

###### How can this feature be enabled / disabled in a live cluster?

- [x] Feature gate
  - Feature gate name: `WorkloadWithDeployment`
  - Components depending on the feature gate:
    - kube-controller-manager
    - kube-apiserver

###### Does enabling the feature change any default behavior?

No. The feature is opt-in via `spec.scheduling`. Deployments without `spec.scheduling` are
unaffected. No scheduling objects are created unless the user explicitly sets the field.

###### Can the feature be disabled once it has been enabled (i.e. can we roll back the enablement)?

Yes. With the gate disabled on kube-apiserver, it clears `spec.scheduling` on creations; with the
gate disabled on kube-controller-manager, the controller stops compiling Workload and PodGroup.
Existing PodGroups remain until their owning ReplicaSet is garbage-collected; the Workload remains
until the Deployment is deleted.

###### What happens if we reenable the feature if it was previously rolled back?

When the feature is re-enabled:
- Deployments with a stored `spec.scheduling` value resume compilation on their next sync.
- Existing Workload/PodGroup objects are discovered via deterministic naming and reused.
- If only a partial set exists (e.g., Workload but no PodGroup from a crash mid-creation), the
  controller completes the missing object on its next sync.

###### Are there any tests for feature enablement/disablement?

Yes. Unit and integration tests cover feature gate on/off behavior.

### Rollout, Upgrade and Rollback Planning

###### How can a rollout or rollback fail? Can it impact already running workloads?

- If the API server doesn't serve the Workload and PodGroup APIs, the Deployment controller fails
  to compile and requeues with backoff until the API server is upgraded.
- Already running Deployments are not affected by enabling the feature; pods already scheduled
  continue to run.
- On rollback, existing Workload/PodGroup objects remain active and pods that already reference a
  PodGroup continue to be gang-scheduled. New pods created after rollback will not have
  `schedulingGroup` set.

###### What specific metrics should inform a rollback?

- `deployment_sync_duration_seconds`: significant increase may indicate issues with Workload/PodGroup
  creation.
- Increased error rate in deployment-controller logs for `scheduling.k8s.io` API calls.

###### Were upgrade and rollback tested? Was the upgrade->downgrade->upgrade path tested?

This will be tested manually as part of alpha release.

###### Is the rollout accompanied by any deprecations and/or removals of features, APIs, fields of API types, flags, etc.?

No.

### Monitoring Requirements

###### How can an operator determine if the feature is in use by workloads?

- `kubectl get workloads -A` will show Workload objects created by the Deployment controller.
- `kubectl get podgroups -A` will show PodGroup objects created by the Deployment controller.

###### How can someone using this feature know that it is working for their instance?

- [x] API .status
  - Condition name: `Available=False` with reason `MinimumReplicasUnavailable` when a gang cannot
    place; `Progressing=False` with reason `ProgressDeadlineExceeded` when placement times out.

###### What are the reasonable SLOs (Service Level Objectives) for the enhancement?

TBD

###### What are the SLIs (Service Level Indicators) an operator can use to determine the health of the service?

TBD

###### Are there any missing metrics that would be useful to have to improve observability of this feature?

A dedicated metric for Workload/PodGroup creation latency per Deployment would be useful for Beta.

### Dependencies

###### Does this feature depend on any specific services running in the cluster?

Yes. The `scheduling.k8s.io` API group must be served (requires `GenericWorkload` feature gate
enabled on the API server).

### Scalability

###### Will enabling / using this feature result in any new API calls?

Yes. The Deployment controller makes direct API calls (no informer in Alpha) for each Deployment
with `spec.scheduling` set:
- `GET Workload` + `CREATE Workload` - 1 per Deployment (created once, reused across ReplicaSets)
- `GET PodGroup` + `CREATE PodGroup` - 1 per new ReplicaSet
- `PATCH PodGroup` - on reown (once per RS creation) and on scale (minCount update)
- `PATCH Workload` - on scale only (template minCount sync); the Workload is never reowned

###### Will enabling / using this feature result in introducing new API types?

No. Workload and PodGroup are introduced by [KEP-6089]; this KEP only creates instances.

###### Will enabling / using this feature result in any new calls to the cloud provider?

No.

###### Will enabling / using this feature result in increasing size or count of the existing API objects?

Yes. Each Deployment with `spec.scheduling` creates 1 Workload per Deployment (~500 bytes) and
1 PodGroup per ReplicaSet (~500 bytes), and each Pod gains a `schedulingGroup` field (~100 bytes).

###### Will enabling / using this feature result in increasing time taken by any operations covered by existing SLIs/SLOs?

There is an expected increase in deployment sync duration due to creating Workload and PodGroup
objects. Impact to be measured during Alpha.

###### Will enabling / using this feature result in non-negligible increase of resource usage (CPU, RAM, disk, IO, ...) in any components?

Minimal for Alpha (opt-in only, no informer). Per-Deployment overhead is one long-lived Workload
plus one PodGroup per live ReplicaSet in etcd. The Workload costs one GET + CREATE once per
Deployment; each new ReplicaSet adds one GET + CREATE for its PodGroup.

###### Can enabling / using this feature result in resource exhaustion of some node resources (PIDs, sockets, inodes, etc.)?

No. This feature is purely control-plane and does not affect node resources.

### Troubleshooting

###### How does this feature react if the API server and/or etcd is unavailable?

- Deployment controller cannot create Workloads/PodGroups.
- Retries with exponential backoff when kube-apiserver recovers.
- Existing Deployments with scheduling objects continue to run.

###### What are other known failure modes?

- Gang cannot place due to insufficient cluster resources: Deployment reports
  `Available=False` / `ProgressDeadlineExceeded`. No automatic recovery.

###### What steps should be taken if SLOs are not being met to determine the problem?

- Verify `WorkloadWithDeployment` and `GenericWorkload` are enabled on all control plane
  components.
- Check controller-manager logs for errors related to Workload/PodGroup creation.
- Check resource constraints since gang scheduling may fail if the cluster doesn't have
  sufficient resources.

## Implementation History

- 2026-08: KEP created for Alpha targeting v1.38.

## Drawbacks

- Rolling updates degenerate to blue/green for full gangs, temporarily doubling peak resource
  consumption during a rollout.
- Permanent topology-domain binding can strand replacement pods in Pending with no automatic
  recovery - the in-tree remedy is triggering a new rollout revision.
- Post-bind runtime failures (e.g., bad image) retain node capacity while individual pods crash;
  the gang holds its reservations even though no useful work is happening.
- Template-backed ResourceClaims double device allocations during rollouts and for the lifetime of
  retained ReplicaSets - with `revisionHistoryLimit=N`, steady state holds (current + N)
  allocations even though only the current revision has running pods.
- Named ResourceClaims pin all revisions to one node, risking deadlock when node capacity is tight
  during rolling updates.
- No informer/lister in Alpha increases API server load relative to a watch-based approach.

## Alternatives

**One PodGroup per Deployment.** Rejected: topology binding is permanent per PodGroup, so a stuck
gang cannot re-place in a different domain without a new revision. Additionally, old and new
rollout gangs would collide within a single PodGroup.

**User-managed PodGroups (status quo).** Rejected: fragile ordering (pods created before PodGroup
hang silently), no garbage collection, and no integration with scaling or rolling updates.

**User-set `minCount`.** Rejected for Alpha: `minCount > replicas` leaves the gang permanently
unsatisfiable with pods Pending forever, and `minCount < replicas` requires multiple PodGroups per
ReplicaSet which is not yet supported. Re-evaluated for Beta.

**ReplicaSet controller creates PodGroups.** Instead of the Deployment controller creating both
Workload and PodGroup, the Deployment controller would create only the Workload, and the
ReplicaSet controller would create the PodGroup before creating pods - using the `workloadbuilder`
library's `NewBuilderFromExistingWorkload` path to derive the PodGroup from the persisted Workload.
The RS controller would detect WAS is active from `schedulingGroup.podGroupName` in the pod
template and look up the Workload by the RS's deterministic name.

*Advantages:* Cleaner ownership - the RS creates and owns the PodGroup directly, eliminating the
bootstrap-own-to-Deployment + reown-to-RS pattern. Each controller manages objects at its own
level of the hierarchy.

*Tradeoffs:* The RS controller is currently a generic "ensure N pods" loop with no awareness of
scheduling or WAS APIs. This approach would add `workloadbuilder` and scheduling API dependencies
to the RS controller and require RBAC grants on `scheduling.k8s.io` resources for the
replicaset-controller.

The current design follows the `workloadbuilder` library's delegated pattern: the Deployment
controller builds the Workload once via `NewBuilder` and stamps each PodGroup from it via
`NewBuilderFromExistingWorkload`. This keeps the RS controller generic and confines all WAS
logic to the Deployment controller.

**One Workload and one PodGroup per ReplicaSet.** Instead of a single shared Workload per
Deployment, the controller would create a fresh Workload for every ReplicaSet revision, with both
the Workload and PodGroup bootstrap-owned by the Deployment and then reowned to the ReplicaSet.

*Advantages:* Workload lifetime is tied to `revisionHistoryLimit` - old Workloads are pruned with
their ReplicaSets, leaving zero long-lived scheduling objects. Each revision is fully self-contained
with its own Workload and PodGroup pair.

*Tradeoffs:* Workload churn on every rollout (two creates plus two reown patches per revision,
versus one PodGroup create plus one reown patch in the adopted design). The Workload's scheduling
policy, topology constraints, and disruption mode are identical across revisions (only `minCount`
changes with scale), so creating a new Workload per RS adds API writes with no semantic benefit.

[KEP-4671]: https://kep.k8s.io/4671
[KEP-5547]: https://kep.k8s.io/5547
[KEP-5710]: https://kep.k8s.io/5710
[KEP-5732]: https://kep.k8s.io/5732
[KEP-6089]: https://kep.k8s.io/6089
