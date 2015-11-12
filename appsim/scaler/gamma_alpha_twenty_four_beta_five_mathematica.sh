#!/usr/local/bin/MathematicaScript -script

(*
This is expected to be run as follows. It requires an argument which sets
the lower bound of the cdf to 'A' and the upper bound as 'B'.

    ./gamma_alpha_twenty_four_beta_five_mathematica.sh 'A=48;B=55'

*)

var = ToExpression[Rest[$ScriptCommandLine]];

Print[Sum[PDF[GammaDistribution[24, 5], x], {x, A, B}]]
