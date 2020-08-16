
/* Lista campos Campos de DSOs 
SAPB03 - troque pelo esquema de seu HANA BW
*/
select  d.INFOAREA
       ,d.ODSOBJECT
       ,i.POSIT
       ,i.KEYFLAG 
       ,i.IOBJNM
       ,it.TXTLG
  from SAPB03.RSDODSO as d inner join
       SAPB03.RSDODSOIOBJ as i on d.ODSOBJECT = i.ODSOBJECT left outer join
       SAPB03.RSDIOBJT as it on it.IOBJNM = i.IOBJNM and it.OBJVERS = i.OBJVERS and it.langu = 'P'
 where     i.OBJVERS = 'A' 
       and d.OBJVERS = 'A' 
       and d.ODSOBJECT = 'DSO_NAME'
order by d.INFOAREA
        ,d.ODSOBJECT
        ,i.POSIT;   