{{- define "hopcorder-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hopcorder-agent.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "hopcorder-agent.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "hopcorder-agent.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "hopcorder-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hopcorder-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
A Kubernetes quantity in bytes, (list name value): digits with an optional
fraction and a k, M, G, T, Ki, Mi, Gi or Ti suffix.
*/}}
{{- define "hopcorder-agent.quantityBytes" -}}
{{- $name := index . 0 }}
{{- $v := toString (index . 1) }}
{{- $num := "^[0-9]+(\\.[0-9]+)?" }}
{{- if not (regexMatch (printf "%s(k|M|G|T|Ki|Mi|Gi|Ti)?$" $num) $v) }}
{{- fail (printf "%s %q: write it in bytes or with a k, M, G, T, Ki, Mi, Gi or Ti suffix (the chart sizes the spool from it)" $name $v) }}
{{- end }}
{{- $unit := get (dict "" 1.0 "k" 1e3 "M" 1e6 "G" 1e9 "T" 1e12 "Ki" 1024.0 "Mi" 1048576.0 "Gi" 1073741824.0 "Ti" 1099511627776.0) (regexReplaceAll $num $v "") }}
{{- mulf (regexFind $num $v | float64) $unit | floor | int64 }}
{{- end }}

{{/*
A size as the agent reads it (config.ByteSize) in bytes, (list name value):
a whole number, with B, KB, MB, GB, TB, KiB, MiB, GiB or TiB after it.
*/}}
{{- define "hopcorder-agent.sizeBytes" -}}
{{- $name := index . 0 }}
{{- $v := index . 1 }}
{{- if or (kindIs "int" $v) (kindIs "int64" $v) (kindIs "float64" $v) }}
{{- $v | int64 }}
{{- else }}
{{- $s := trim (toString $v) }}
{{- if not (regexMatch "^[0-9]+ *(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB)?$" $s) }}
{{- fail (printf "%s %q: a size such as 768MiB or 1GiB" $name $s) }}
{{- end }}
{{- $unit := get (dict "" 1 "B" 1 "KB" 1000 "MB" 1000000 "GB" 1000000000 "TB" 1000000000000 "KiB" 1024 "MiB" 1048576 "GiB" 1073741824 "TiB" 1099511627776) (regexReplaceAll "^[0-9]+ *" $s "") }}
{{- mul (regexFind "^[0-9]+" $s | int64) $unit }}
{{- end }}
{{- end }}

{{/* The configuration file: the user's config plus what the chart owns. */}}
{{- define "hopcorder-agent.config" -}}
{{- if not .Values.config.targets }}
{{- fail "config.targets: list at least one target, e.g. --set 'config.targets[0].host=example.net'" }}
{{- end }}
{{- if not (has .Values.mode (list "statefulset" "daemonset")) }}
{{- fail "mode: want statefulset or daemonset" }}
{{- end }}
{{- if not .Values.server.url }}
{{- fail "server.url: set the hopcorder server's URL" }}
{{- end }}
{{- if regexMatch "^[A-Za-z][A-Za-z0-9+.-]*://[^/?#]*@" .Values.server.url }}
{{- fail "server.url: no credentials in the URL (it would land in a ConfigMap); use server.basicAuth" }}
{{- end }}
{{- $http := default dict (default dict .Values.config.outputs).http }}
{{- if or $http.bearer_token $http.bearer_token_file $http.basic_auth $http.tls }}
{{- fail "config.outputs.http: set credentials and TLS with server.bearerTokenSecret, server.basicAuth and server.tls (from Secrets), not in config" }}
{{- end }}
{{- /* Headers that carry credentials, matched by name in any case: they would
  land in the ConfigMap. A heuristic, not a guarantee: names containing
  token, secret, passw(or)d, credential, cookie, signature, session, jwt,
  bearer, api-key, authori[sz]ation or authentication anywhere
  (X-AccessToken, Proxy-Authorization, X-Jwt), and names with a part key(s),
  auth, pass or sig (X-Key, X-Auth). */}}
{{- range $name, $_ := (default dict $http.headers) }}
{{- if regexMatch "(token|secret|passw|credential|cookie|signature|session|jwt|bearer|api[-_]?key|authori[sz]|authent)|(^|[-_])(keys?|auth|pass|sig)([-_]|$)" (lower $name) }}
{{- fail (printf "config.outputs.http.headers.%s: looks like a credential; in config it would land in a ConfigMap. Use server.bearerTokenSecret, server.basicAuth or server.tls" $name) }}
{{- end }}
{{- end }}
{{- $cfg := deepCopy .Values.config }}
{{- $spool := default dict $cfg.spool }}
{{- $_ := set $spool "dir" "/var/lib/hopcorder" }}
{{- /* The spool's bound on its volume (review, round six): the spool file
  grows to max_bytes and no further, and the file system needs the rest.
  Unset, 75% of persistence.size, at most the agent's default (1GiB); set,
  at most 90% of it. mode: daemonset has no volume to check. */}}
{{- if eq .Values.mode "statefulset" }}
{{- $vol := include "hopcorder-agent.quantityBytes" (list "persistence.size" .Values.persistence.size) | int64 }}
{{- if $spool.max_bytes }}
{{- $mb := include "hopcorder-agent.sizeBytes" (list "config.spool.max_bytes" $spool.max_bytes) | int64 }}
{{- if lt $mb 1048576 }}
{{- fail (printf "config.spool.max_bytes %v: at least 1MiB (the agent refuses less)" $spool.max_bytes) }}
{{- end }}
{{- if gt (mul $mb 10) (mul $vol 9) }}
{{- fail (printf "config.spool.max_bytes %v is more than 90%% of persistence.size %v: the spool file grows to max_bytes, and the file system needs the rest. Lower max_bytes (or leave it unset: 75%% of the volume), or give new volumes more (persistence.size). Upgrading with --reuse-values or a values file from an earlier chart, whose default was 1GiB: add --set config.spool.max_bytes=null, or use --reset-then-reuse-values" $spool.max_bytes .Values.persistence.size) }}
{{- end }}
{{- else }}
{{- $mib := min 1024 (div (mul $vol 3) (mul 4 1048576)) }}
{{- if lt $mib 1 }}
{{- fail (printf "persistence.size %v: too small for the spool (at least 2Mi)" .Values.persistence.size) }}
{{- end }}
{{- $_ := set $spool "max_bytes" (printf "%dMiB" $mib) }}
{{- end }}
{{- end }}
{{- $_ := set $cfg "spool" $spool }}
{{- $out := default dict $cfg.outputs }}
{{- $http := default dict $out.http }}
{{- $_ := set $http "url" .Values.server.url }}
{{- if .Values.server.bearerTokenSecret.name }}
{{- $_ := set $http "bearer_token_file" "/etc/hopcorder/secrets/token" }}
{{- end }}
{{- with .Values.server.basicAuth }}
{{- if .username }}
{{- if not .passwordSecret.name }}
{{- fail "server.basicAuth: set passwordSecret.name" }}
{{- end }}
{{- $_ := set $http "basic_auth" (dict "username" .username "password_file" "/etc/hopcorder/basic-auth/password") }}
{{- end }}
{{- end }}
{{- with .Values.server.tls }}
{{- if .secretName }}
{{- $tls := dict }}
{{- if .caKey }}{{- $_ := set $tls "ca_file" "/etc/hopcorder/tls/ca.crt" }}{{- end }}
{{- if .certKey }}{{- $_ := set $tls "cert_file" "/etc/hopcorder/tls/tls.crt" }}{{- end }}
{{- if .keyKey }}{{- $_ := set $tls "key_file" "/etc/hopcorder/tls/tls.key" }}{{- end }}
{{- if .serverName }}{{- $_ := set $tls "server_name" .serverName }}{{- end }}
{{- $_ := set $http "tls" $tls }}
{{- end }}
{{- end }}
{{- $_ := set $out "http" $http }}
{{- if .Values.metrics.enabled }}
{{- $_ := set $out "metrics" (dict "listen" (printf "%s:%d" .Values.metrics.listenAddress (int .Values.metrics.port))) }}
{{- end }}
{{- $_ := set $cfg "outputs" $out }}
{{- toYaml $cfg }}
{{- end }}

{{/*
The --agent-id argument: agentIdPrefix, then the pod's name (statefulset) or
the node's (daemonset), which the kubelet puts into $(AGENT_ID). Agent IDs
must be unique per server, and pod and node names repeat across clusters
(review, round seven). The agent takes 1 to 128 characters from
[A-Za-z0-9._:-] (record.ValidAgentID), so the chart refuses a prefix with
any other character (none can start a $(VAR) for the kubelet, either), or
one too long with the last pod's name or a node name of one character.
*/}}
{{- define "hopcorder-agent.agentId" -}}
{{- $prefix := toString (.Values.agentIdPrefix | default "") }}
{{- if not (regexMatch "^[A-Za-z0-9._:-]*$" $prefix) }}
{{- fail (printf "agentIdPrefix %q: letters, digits, '.', '_', ':' and '-' only (an agent_id has no others)" $prefix) }}
{{- end }}
{{- if eq .Values.mode "statefulset" }}
{{- $pod := printf "%s-%d" (include "hopcorder-agent.fullname" .) (sub (max 1 (int .Values.replicas)) 1) }}
{{- if gt (add (len $prefix) (len $pod)) 128 }}
{{- fail (printf "agentIdPrefix (%d characters) and the pod name %s (%d): an agent_id has at most 128 characters" (len $prefix) $pod (len $pod)) }}
{{- end }}
{{- else if gt (len $prefix) 127 }}
{{- fail (printf "agentIdPrefix (%d characters) and the node's name: an agent_id has at most 128 characters" (len $prefix)) }}
{{- end }}
{{- printf "%s$(AGENT_ID)" $prefix }}
{{- end }}

{{/* The pod template, shared by the StatefulSet and the DaemonSet. */}}
{{- define "hopcorder-agent.podTemplate" -}}
metadata:
  annotations:
    checksum/config: {{ include "hopcorder-agent.config" . | sha256sum }}
    {{- with .Values.podAnnotations }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
  labels:
    {{- include "hopcorder-agent.selectorLabels" . | nindent 4 }}
    {{- with .Values.podLabels }}
    {{- toYaml . | nindent 4 }}
    {{- end }}
spec:
  {{- with .Values.imagePullSecrets }}
  imagePullSecrets:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  # The agent does not talk to the Kubernetes API.
  automountServiceAccountToken: false
  hostNetwork: {{ .Values.hostNetwork }}
  {{- if .Values.hostNetwork }}
  dnsPolicy: ClusterFirstWithHostNet
  {{- end }}
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  containers:
    - name: agent
      {{- /* The chart's own version by digest (released charts carry it),
      or the tag given. */}}
      {{- $digest := .Values.image.digest | default "" }}
      {{- if and $digest (not (regexMatch "^sha256:[0-9a-f]{64}$" $digest)) }}
      {{- fail (printf "image.digest: %q is not sha256:<64 hex digits>" $digest) }}
      {{- end }}
      {{- $tag := .Values.image.tag }}
      {{- if not $tag }}
      {{- $tag = .Chart.AppVersion }}
      {{- if $digest }}
      {{- $tag = printf "%s@%s" $tag $digest }}
      {{- end }}
      {{- end }}
      image: "{{ .Values.image.repository }}:{{ $tag }}"
      imagePullPolicy: {{ .Values.image.pullPolicy }}
      env:
        - name: AGENT_ID
          valueFrom:
            fieldRef:
              # daemonset: the node; statefulset: the stable pod name.
              fieldPath: {{ ternary "spec.nodeName" "metadata.name" (eq .Values.mode "daemonset") }}
      args:
        - --config
        - /etc/hopcorder/agent.yaml
        - --agent-id
        - {{ include "hopcorder-agent.agentId" . | quote }}
        {{- with .Values.extraArgs }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- /* No liveness probe: /metrics answering says nothing about the
        probing loop, and its listener holds at most 64 connections, so
        anything that can reach the port could hold them and have the agent
        restarted over and over. */}}
      {{- if and .Values.metrics.enabled (ne .Values.metrics.listenAddress "127.0.0.1") }}
      ports:
        - name: metrics
          containerPort: {{ .Values.metrics.port }}
          protocol: TCP
      {{- end }}
      resources:
        {{- toYaml .Values.resources | nindent 8 }}
      securityContext:
        {{- toYaml .Values.securityContext | nindent 8 }}
      volumeMounts:
        - name: config
          mountPath: /etc/hopcorder/agent.yaml
          subPath: agent.yaml
          readOnly: true
        - name: spool
          mountPath: /var/lib/hopcorder
        {{- if .Values.server.bearerTokenSecret.name }}
        - name: token
          mountPath: /etc/hopcorder/secrets
          readOnly: true
        {{- end }}
        {{- if .Values.server.basicAuth.username }}
        - name: basic-auth
          mountPath: /etc/hopcorder/basic-auth
          readOnly: true
        {{- end }}
        {{- if .Values.server.tls.secretName }}
        - name: tls
          mountPath: /etc/hopcorder/tls
          readOnly: true
        {{- end }}
  volumes:
    - name: config
      configMap:
        name: {{ include "hopcorder-agent.fullname" . }}
    {{- if eq .Values.mode "daemonset" }}
    - name: spool
      hostPath:
        path: {{ .Values.hostPath }}
        type: DirectoryOrCreate
    {{- end }}
    {{- if .Values.server.bearerTokenSecret.name }}
    - name: token
      secret:
        secretName: {{ .Values.server.bearerTokenSecret.name }}
        items:
          - key: {{ .Values.server.bearerTokenSecret.key }}
            path: token
    {{- end }}
    {{- if .Values.server.basicAuth.username }}
    - name: basic-auth
      secret:
        secretName: {{ .Values.server.basicAuth.passwordSecret.name }}
        items:
          - key: {{ .Values.server.basicAuth.passwordSecret.key }}
            path: password
    {{- end }}
    {{- with .Values.server.tls }}
    {{- if .secretName }}
    - name: tls
      secret:
        secretName: {{ .secretName }}
        items:
          {{- if .caKey }}
          - key: {{ .caKey }}
            path: ca.crt
          {{- end }}
          {{- if .certKey }}
          - key: {{ .certKey }}
            path: tls.crt
          {{- end }}
          {{- if .keyKey }}
          - key: {{ .keyKey }}
            path: tls.key
          {{- end }}
    {{- end }}
    {{- end }}
  {{- with .Values.nodeSelector }}
  nodeSelector:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.affinity }}
  affinity:
    {{- toYaml . | nindent 4 }}
  {{- end }}
  {{- with .Values.tolerations }}
  tolerations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
