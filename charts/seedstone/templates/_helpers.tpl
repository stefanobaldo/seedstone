{{- define "seedstone.name" -}}{{ .Chart.Name }}{{- end -}}
{{- define "seedstone.fullname" -}}{{ printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "seedstone.labels" -}}
app.kubernetes.io/name: {{ include "seedstone.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}
{{- define "seedstone.selector" -}}
app.kubernetes.io/name: {{ include "seedstone.name" . }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}
{{- define "seedstone.image" -}}
{{- if .Values.image.digest -}}{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default (printf "v%s" .Chart.AppVersion) }}{{- end -}}
{{- end -}}
{{- define "seedstone.secretName" -}}{{ .Values.auth.existingSecret | default (include "seedstone.fullname" .) }}{{- end -}}
