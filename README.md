%if %upcase(&extrap_depdmd.) = DEP_AVG %then %do;

	    proc sql;
	        create table work.last_24_months_avg as
	        select NIIN, sum(depdmdqty)/24 as DEP_AVG
	        from work.depdmdstaticdata_clean
	        where MonthID <= 23
	        group by NIIN;
	    quit;
 
	    proc sql;
	        create table &outdsn1. as
	        select a.NIIN, a.MonthID,
	               /* Apply DEP_AVG if MonthID is greater than the duration of the available data */
	               case when a.MonthID > 23 then b.DEP_AVG else a.depdmdqty end as depdmdqty
	        from work.depdmdstaticdata_clean a
	        left join work.last_24_months_avg b
	        on a.NIIN = b.NIIN
	        order by niin, monthid;
	    quit;

    %end;

    %else %if %upcase(&extrap_depdmd.) = DEP_LOOP %then %do;

		data &outdsn1.;
		    set work.depdmdstaticdata_clean;
		    by NIIN Date;
		    array first_24_months{24} _temporary_;
		    retain month_id;
		
		    if first.NIIN then do;
				month_id = 0;
		    end;
	
			month_id + 1;
		
		    if month_id <= 24 then do;
				 first_24_months{month_id} = depdmdqty;
		    end;
		    else do;
		        depdmdqty = first_24_months{mod(month_id-1, 24) + 1};
		    end;
		
		run;
		
	%end;

	* Transpose depdmdqty to separate variables for each MonthID;
	proc transpose data=&outdsn1. out=&outdsn1. prefix=depdmdqty;
	    by NIIN; 
		var depdmdqty; 
		id MonthID;
	run;
