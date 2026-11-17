{{- define "hopcorder-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "hopcorder-server.fullname" -}}
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

{{- define "hopcorder-server.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
{{ include "hopcorder-server.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "hopcorder-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "hopcorder-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* The bundled ClickHouse (clickhouse.deploy): its name and labels. */}}
{{- define "hopcorder-server.clickhouseName" -}}
{{- printf "%s-clickhouse" (include "hopcorder-server.fullname" . | trunc 52 | trimSuffix "-") }}
{{- end }}

{{- define "hopcorder-server.clickhouseSelectorLabels" -}}
app.kubernetes.io/name: clickhouse
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: hopcorder-clickhouse
{{- end }}

{{/* The Secret holding the server's ClickHouse password, if there is one. */}}
{{- define "hopcorder-server.clickhousePasswordSecret" -}}
{{- if .Values.clickhouse.passwordSecret.name }}
{{- .Values.clickhouse.passwordSecret.name }}
{{- else if .Values.clickhouse.deploy.enabled }}
{{- include "hopcorder-server.clickhouseName" . }}
{{- end }}
{{- end }}

{{/* The key of the server's ClickHouse password in that Secret. */}}
{{- define "hopcorder-server.clickhousePasswordKey" -}}
{{- if .Values.clickhouse.passwordSecret.name }}{{ .Values.clickhouse.passwordSecret.key }}{{ else }}password{{ end }}
{{- end }}

{{/* The Secret holding the bundled ClickHouse's read-only user's password. */}}
{{- define "hopcorder-server.clickhouseReadOnlySecret" -}}
{{- .Values.clickhouse.deploy.readOnlyUser.passwordSecret.name | default (printf "%s-readonly" (include "hopcorder-server.clickhouseName" .)) }}
{{- end }}

{{- define "hopcorder-server.clickhouseReadOnlyKey" -}}
{{- if .Values.clickhouse.deploy.readOnlyUser.passwordSecret.name }}{{ .Values.clickhouse.deploy.readOnlyUser.passwordSecret.key }}{{ else }}password{{ end }}
{{- end }}

{{/*
A generated password: the existing Secret's value if there is one (so an
upgrade keeps it), else a new random one. Takes (list $ secretName).
*/}}
{{- define "hopcorder-server.generatedPassword" -}}
{{- $root := index . 0 }}
{{- $existing := lookup "v1" "Secret" $root.Release.Namespace (index . 1) }}
{{- if and $existing $existing.data $existing.data.password }}
{{- $existing.data.password }}
{{- else }}
{{- randAlphaNum 32 | b64enc }}
{{- end }}
{{- end }}

{{/*
The Host names the server answers, against DNS rebinding (allowedHosts in
values.yaml), as a YAML list: the localhost names (kubectl port-forward), the
Service's names, the ingress hosts and allowedHosts; or ["*"] (no check) if
allowedHosts has "*". With an ingress rule without host, or a NodePort or
LoadBalancer Service, the chart cannot know the names clients use, so
allowedHosts must list them.
*/}}
{{- define "hopcorder-server.allowedHosts" -}}
{{- $extra := .Values.allowedHosts | default list }}
{{- if has "*" $extra }}
{{- list "*" | toYaml }}
{{- else }}
{{- $full := include "hopcorder-server.fullname" . }}
{{- $ns := .Release.Namespace }}
{{- $hosts := list "localhost" "127.0.0.1" "::1" $full (printf "%s.%s" $full $ns) (printf "%s.%s.svc" $full $ns) (printf "%s.%s.svc.%s" $full $ns (.Values.clusterDomain | default "cluster.local")) }}
{{- if .Values.ingress.enabled }}
{{- range .Values.ingress.hosts }}
{{- if .host }}
{{- $hosts = append $hosts .host }}
{{- else if not $extra }}
{{- fail "ingress.hosts: a rule without host answers any name: list the names clients use in allowedHosts (the server answers only the names it knows, against DNS rebinding), or set allowedHosts to [\"*\"] to turn that check off" }}
{{- end }}
{{- end }}
{{- end }}
{{- if and (has .Values.service.type (list "NodePort" "LoadBalancer")) (not $extra) }}
{{- fail (printf "service.type %s: list the names and addresses clients use for it in allowedHosts (the server answers only the names it knows, against DNS rebinding), or set allowedHosts to [\"*\"] to turn that check off" .Values.service.type) }}
{{- end }}
{{- range $h := concat $hosts $extra }}
{{- if not (regexMatch "^((\\*\\.)?[A-Za-z0-9_-][A-Za-z0-9._-]*|\\[?[0-9A-Fa-f:.]+\\]?)$" (toString $h)) }}
{{- fail (printf "allowedHosts: %q is not a host name (or *.domain) or IP address without port" (toString $h)) }}
{{- end }}
{{- end }}
{{- concat $hosts $extra | uniq | toYaml }}
{{- end }}
{{- end }}

{{/* The configuration file: the user's config plus what the chart owns. */}}
{{- define "hopcorder-server.config" -}}
{{- if (default dict .Values.config.clickhouse).password }}
{{- fail "config.clickhouse.password: put the password in a Secret and set clickhouse.passwordSecret.name (in config it would land in a ConfigMap)" }}
{{- end }}
{{- $cfg := deepCopy .Values.config }}
{{- $_ := set $cfg "listen" (printf "0.0.0.0:%d" (int .Values.service.port)) }}
{{- if not $cfg.allowed_hosts }}
{{- $_ := set $cfg "allowed_hosts" (include "hopcorder-server.allowedHosts" . | fromYamlArray) }}
{{- end }}
{{- $ch := default dict $cfg.clickhouse }}
{{- if .Values.clickhouse.deploy.enabled }}
{{- $_ := set $ch "url" (printf "http://%s:8123" (include "hopcorder-server.clickhouseName" .)) }}
{{- $_ := set $ch "user" .Values.clickhouse.deploy.user }}
{{- end }}
{{- if include "hopcorder-server.clickhousePasswordSecret" . }}
{{- $_ := set $ch "password_file" "/etc/hopcorder/secrets/clickhouse-password" }}
{{- end }}
{{- $_ := set $cfg "clickhouse" $ch }}
{{- $enrich := default dict $cfg.enrich }}
{{- $asn := default dict $enrich.asn }}
{{- $_ := set $asn "file" "/var/lib/hopcorder-server/ip2asn-combined.tsv.gz" }}
{{- $_ := set $enrich "asn" $asn }}
{{- $_ := set $cfg "enrich" $enrich }}
{{- toYaml $cfg }}
{{- end }}
