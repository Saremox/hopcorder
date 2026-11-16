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
{{- $cfg := deepCopy .Values.config }}
{{- $spool := default dict $cfg.spool }}
{{- $_ := set $spool "dir" "/var/lib/hopcorder" }}
{{- $_ := set $cfg "spool" $spool }}
{{- $out := default dict $cfg.outputs }}
{{- $http := default dict $out.http }}
{{- $_ := set $http "url" .Values.server.url }}
{{- if .Values.server.bearerTokenSecret.name }}
{{- $_ := set $http "bearer_token_file" "/etc/hopcorder/secrets/token" }}
{{- end }}
{{- $_ := set $out "http" $http }}
{{- if .Values.metrics.enabled }}
{{- $_ := set $out "metrics" (dict "listen" (printf "0.0.0.0:%d" (int .Values.metrics.port))) }}
{{- end }}
{{- $_ := set $cfg "outputs" $out }}
{{- toYaml $cfg }}
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
  hostNetwork: {{ .Values.hostNetwork }}
  {{- if .Values.hostNetwork }}
  dnsPolicy: ClusterFirstWithHostNet
  {{- end }}
  securityContext:
    {{- toYaml .Values.podSecurityContext | nindent 4 }}
  containers:
    - name: agent
      image: "{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}"
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
        - $(AGENT_ID)
        {{- with .Values.extraArgs }}
        {{- toYaml . | nindent 8 }}
        {{- end }}
      {{- if .Values.metrics.enabled }}
      ports:
        - name: metrics
          containerPort: {{ .Values.metrics.port }}
          protocol: TCP
      livenessProbe:
        httpGet:
          path: /metrics
          port: metrics
        periodSeconds: 30
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
