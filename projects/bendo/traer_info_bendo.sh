#!/bin/env bash

# Crea una nueva sesión de tmux en segundo plano
tmux new-session -d -s mi_sesion

# Ejecuta el primer comando en el primer panel
tmux send-keys "poetry run extraer traer --sql balances.sql --ruta-credenciales ~/configs/estrategia.env --verbose" C-m

# Divide la pantalla en 2 paneles (horizontal)
tmux split-window -h

# Ejecuta el segundo comando en el segundo panel
tmux send-keys "poetry run extraer traer --sql facturacion.sql --ruta-credenciales ~/configs/data_fact.env --verbose" C-m

# Divide el panel izquierdo en 2 (vertical), ahora tenemos 3 paneles
tmux split-window -v

# Ejecuta el tercer comando en el tercer panel
tmux send-keys "poetry run extraer traer --sql contactabilidad.sql --ruta-credenciales ~/configs/data_fact.env --verbose" C-m

# Selecciona el cuarto panel (el único que falta, abajo a la derecha)
tmux select-pane -t 3

# Ejecuta el cuarto comando en el cuarto panel
tmux send-keys "poetry run extraer traer --sql informacion_general.sql --ruta-credenciales ~/configs/data_fact.env --verbose" C-m

# Adjunta a la sesión de tmux para ver la salida
tmux attach -t mi_sesion
