
/* Campos de InfoCubo BW 
   Troque SAPB03 pelo esquema de seu BW*/
select * from 
(
select  i.INFOAREA
       ,i.INFOCUBE
       ,d.DIMENSION
       ,dt.TXTLG as DIM_NAME
       ,di.POSIT
       ,di.IOBJNM
       ,IFNULL(it.TXTLG, IFNULL(nt.TXTLG, it2.TXTLG)) as IOBJTXT
  from SAPB03.RSDCUBE as i inner join
       SAPB03.RSDDIME as d on (d.INFOCUBE = i.INFOCUBE 
                               and d.OBJVERS = i.OBJVERS) inner join 
       SAPB03.RSDDIMEIOBJ as di on (di.INFOCUBE = d.INFOCUBE 
			                        and di.OBJVERS = d.OBJVERS 
			                        and di.DIMENSION = d.DIMENSION) left outer join
       SAPB03.RSDDIMET as dt  on (dt.INFOCUBE = d.INFOCUBE 
		                          and dt.OBJVERS = d.OBJVERS
		                          and dt.DIMENSION = d.DIMENSION
		                          and dt.langu = 'P') left outer join
       SAPB03.RSDIOBJT as it on (it.IOBJNM = di.IOBJNM 
                                 and it.OBJVERS = i.OBJVERS 
                                 and it.langu = 'P')  left outer join
                                 
       SAPB03.RSDATRNAV as na on (na.ATRNAVNM = di.IOBJNM
                                  and na.OBJVERS = i.OBJVERS )  left outer join
                                  
       SAPB03.RSDATRNAVT as nt on (nt.ATTRINM = na.ATTRINM 
                                   and nt.CHANM = na.CHANM
                                   and nt.LANGU = 'P'
                                   and nt.OBJVERS = i.OBJVERS )  left outer join
                                   
       SAPB03.RSDIOBJT as it2 on (it2.IOBJNM = na.ATTRINM 
                                 and it2.OBJVERS = i.OBJVERS 
                                 and it2.langu = 'P')
                                   