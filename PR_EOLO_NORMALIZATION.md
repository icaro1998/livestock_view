## Contexto

Este cambio introduce .gitattributes para normalizar finales de línea en todo el repositorio y evitar warnings recurrentes de LF/CRLF en Windows.

## Qué cambia

- Se agrega .gitattributes con reglas:
  - * text=auto eol=lf para archivos de texto.
  - *.bat, *.cmd, *.ps1 en CRLF por compatibilidad Windows.
  - formatos binarios marcados como binary.

## Impacto esperado

- No hay cambios funcionales de lógica.
- El diff es grande porque Git renormalizó archivos existentes (cambio de EOL).
- Reduce ruido en futuros commits y evita conversiones inconsistentes entre entornos.

## Commit relacionado

- ac84adb — Add .gitattributes and normalize line endings
