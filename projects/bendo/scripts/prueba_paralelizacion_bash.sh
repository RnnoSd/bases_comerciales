#!/bin/env bash

# Inicia una nueva sesión tmux
tmux new-session -d -s mi_sesion

# Crear y dividir la pantalla en 4 paneles
tmux split-window -h  # Divide la ventana en 2 paneles (horizontal)
tmux split-window -v  # Divide el panel izquierdo en 2 (vertical)
tmux select-pane -t 0  # Asegura que estamos en el primer panel

# Ejecuta el comando echo en cada panel
tmux send-keys "echo 'Pantalla 1'" C-m
tmux select-pane -t 1
tmux send-keys "echo 'Pantalla 2'" C-m
tmux select-pane -t 2
tmux send-keys "echo 'Pantalla 3'" C-m
tmux select-pane -t 3
tmux send-keys "echo 'Pantalla 4'" C-m

# Adjuntar a la sesión tmux
tmux attach -t mi_sesion
