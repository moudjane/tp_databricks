# Groupe 2 : Mathis OUDJANE, Nao MAUSSERVEY, Jonathan HAYOT

## Théorie : 
Les performances des clubs de football influencent-elles la valeur des joueurs qui y sont affiliés ?

## Présentation de la structure :
`games/` : Contient les données relatives aux matchs, incluant les scores, les équipes participantes et d'autres métriques pertinentes.<br>
`player_valuations/` : Regroupe les données sur les valorisations financières des joueurs sur différentes périodes.<br>
`players/` : Inclut les informations détaillées sur les joueurs, telles que leur identité et leur affiliation aux clubs.<br>
`notebook_games.py`, `notebook_players.py` et `notebook_player_valuation.py` sont les scripts qui transforment les CSV games, players et player_valuations bronzes en silver et génère les CSV de façon nettoyés.<br>
Puis on a le script dans le dossier gold/ `notebook_gold.py` qui transforme les fichiers CSV de la couche Silver en tables Delta.<br><br>

Le rapport est disponible en PDF `TP_Databricks__MathisOUDJANE_NaoMAUSSERVEY_JonathanHAYOT.pdf`