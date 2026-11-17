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

{{/* The configuration file: the user's config plus what the chart owns. */}}
{{- define "hopcorder-server.config" -}}
{{- if (default dict .Values.config.clickhouse).password }}
{{- fail "config.clickhouse.password: put the password in a Secret and set clickhouse.passwordSecret.name (in config it would land in a ConfigMap)" }}
{{- end }}
{{- $cfg := deepCopy .Values.config }}
{{- $_ := set $cfg "listen" (printf "0.0.0.0:%d" (int .Values.service.port)) }}
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
