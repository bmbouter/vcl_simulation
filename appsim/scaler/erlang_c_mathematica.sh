#!/usr/local/bin/MathematicaScript -script

(*
This is expected to be run as follows. It requires an argument which sets
lambda as 'A' and the capacity as 'M'.

    ./erlang_c_mathematica.sh 'A=48;M=55'

*)

var = ToExpression[Rest[$ScriptCommandLine]];

Print[N[((A^M)/M!)/((Sum[(A^i)/i!, {i, 0, M - 1}]*(1 - (A/M))) + ((A^M)/M!))]]
