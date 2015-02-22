function [y_hat,resid,PR,PR_STT,PR_STL]=run_ms_var(grocer_res_inp,grocer_endo,varargin)
    

grocer_dropna=%f
grocer_Vec_Mat_foo=MSVAR_Vec_Mat

grocer_MS_K=grocer_res_inp('n_z')
grocer_MS_p=grocer_res_inp('nlag')
grocer_MS_M=grocer_res_inp('nb_states')
grocer_MS_M_V=grocer_res_inp('switching V')
grocer_MS_var_opt=grocer_res_inp('var_opt')
grocer_param=grocer_res_inp('coeff')
grocer_MS_typmod=grocer_res_inp('typemod')

if grocer_MS_M_V==1 then
   grocer_MS_MlikeMV=ones(1,grocer_MS_M)
else
   grocer_MS_MlikeMV=1
end

if exists('grocer_boundsvar') then
   // get the bounds p times back, because the y variable in var1
   // is lead p times
   // transfer the boundsvar variable in the function
   // so that it can be transformed in the function
   if size(grocer_boundsvar,1) > 2 then
      error('you cannot use discountinous bounds in var')
   end
   if grocer_boundsvar ~= [] then
      grocer_boundsvar=[num2date(date2num(grocer_boundsvar(1))-grocer_MS_p,...
                   date2fq(grocer_boundsvar(1))) ; grocer_boundsvar(2)]
   end
end
 
[grocer_yall,grocer_namey,grocer_prests,grocer_b]=explone(grocer_endo,[],'endogenous',%t,grocer_dropna)

ylag = mlagb(grocer_yall,grocer_MS_p)
ylag = ylag(grocer_MS_p+1:$,:)
grocer_y=grocer_yall(grocer_MS_p+1:$,:)
[grocer_MS_T,grocer_MS_K]=size(grocer_y)
 
grocer_names_ylag=[]
for k=1:grocer_MS_p
   for j=1:grocer_MS_K
      grocer_names_ylag=[grocer_names_ylag ; grocer_namey(j)+'(-'+string(k)+')']
   end
end

if grocer_MS_typmod == 2 then
   nz=zeros(grocer_MS_K,1)
   nx=(grocer_MS_p*grocer_MS_K+1)*ones(grocer_MS_K,1)
   grocer_MS_typmod=2
   grocer_x=eye(grocer_MS_K,grocer_MS_K) .*. [ylag ones(grocer_MS_T,1)]
   grocer_z = []
   namex0=[grocer_names_ylag ; 'const']
   namex=namex0
   for i=2:grocer_MS_K
      namex=[namex ; namex0 ]
   end
   namez=[]

elseif grocer_MS_typmod == 3 then
   nz=grocer_MS_p*grocer_MS_K*ones(grocer_MS_K,1)
   nx=ones(grocer_MS_K,1)
   grocer_x = eye(grocer_MS_K,grocer_MS_K) .*. ones(grocer_MS_T,1) 
   grocer_z = eye(grocer_MS_K,grocer_MS_K) .*. ylag
   namex0=['const']
   namex=namex0
   for i=2:grocer_MS_K
      namex=[namex ; namex0 ]
   end
   namez=grocer_names_ylag
   for i=2:grocer_MS_K
      namez=[namez ; grocer_names_ylag ]
   end
else
   error('not a valid typemod for ms input tlist')
end
y_mat=matrix(grocer_y,grocer_MS_T*grocer_MS_K,1)

[Likv,y_hat,resid,PR,PR_STT,PR_STL] = MSVAR_Filt(grocer_param,y_mat,grocer_x,grocer_z,grocer_MS_M,grocer_MS_M_V,grocer_MS_var_opt,grocer_MS_typmod)

 
endfunction
