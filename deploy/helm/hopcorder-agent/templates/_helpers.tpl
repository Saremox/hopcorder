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
