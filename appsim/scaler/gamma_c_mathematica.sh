#!/usr/local/bin/MathematicaScript -script

(*
This is expected to be run as follows. It requires an argument which sets
the lower bound of the cdf to 'A' and the upper bound as 'B'.

    ./erlang_c_mathematica.sh 'A=48;B=55'

*)

var = ToExpression[Rest[$ScriptCommandLine]];

Print[Sum[PDF[GammaDistribution[1.1401352417216322`, 4158.338989087202`], x], {x, A, B}]]
