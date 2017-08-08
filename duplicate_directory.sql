if object_id('RefPartners_Units_temp_1') is not null
  drop table [dbo].[RefPartners_Units_temp_1];
go

--создаем таблицу для дубля справочника контрагентов
create table [dbo].[RefPartners_Units_temp_1] (
  RowID 	 uniqueidentifier not null,
  NameUnitNew   nvarchar(420) null,
  NameUnit nvarchar(420) null);
go

--создаем индексы для таблицы RefPartners_Units_temp_1
create clustered index [RefPartners_Units_temp_1_RowID] 
                    on RefPartners_Units_temp_1 (RowID);
create nonclustered index [RefPartners_Units_temp_1_Name] 
                       on RefPartners_Units_temp_1 (NameUnitNew);
go

if object_id('RefPartners_Units_temp_2') is not null
  drop table [dbo].[RefPartners_Units_temp_2];
go

--создаем таблицу для группировки совпадений в именах
create table [dbo].[RefPartners_Units_temp_2] (
  RUNKROW int,
  RowID 	 uniqueidentifier not null,
  NameUnit nvarchar(420) null);
go

--создаем индексы для таблицы RefPartners_Units_temp_2
create clustered index [RefPartners_Units_temp_2_RowID] 
                    on RefPartners_Units_temp_2 (RowID);
create nonclustered index [RefPartners_Units_temp_2_Name] 
                       on RefPartners_Units_temp_2 (NameUnit);
go

if object_id('RefPartners_Units_temp_3') is not null
  drop table [dbo].[RefPartners_Units_temp_3];
go

--создаем таблицу для эталонных контрагентов и их дублей
create table [dbo].[RefPartners_Units_temp_3] (
  ROWNUM int,
  COUNTROW int,
  RUNKROW int,
  RowID 	 uniqueidentifier not null,
  NameUnit nvarchar(420) null); 
go

--создаем индексы для таблицы RefPartners_Units_temp_3
create clustered index [RefPartners_Units_temp_3_RowID] 
                    on RefPartners_Units_temp_3 (RowID);
create nonclustered index [RefPartners_Units_temp_3_Name] 
                       on RefPartners_Units_temp_3 (NameUnit);
go

--копируем всех контрагентов и реплейсим имена
insert into RefPartners_Units_temp_1
select 
	RowID
	,REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(UPPER(RefPartners_Units.Name),' ',''),'"',''),'_',''),'.',''),',',''),'-',''),'(',''),')',''),'[',''),']',''),'!',''),'?',''),'«',''),'»',''),'+','') as NameUnitNew
	,Name as NameUnit
	from [dvtable_{C78ABDED-DB1C-4217-AE0D-51A400546923}] RefPartners_Units (nolock);
go

--групперуем похожие имена и заносим в таблицу RefPartners_Units_temp_2
insert into RefPartners_Units_temp_2
select  
			DENSE_RANK() OVER (ORDER BY RefPartners_Units1.NameUnitNew) RUNKROW
			,RefPartners_Units1.RowID
			,RefPartners_Units1.NameUnit
	from RefPartners_Units_temp_1 RefPartners_Units1 (nolock)
	join RefPartners_Units_temp_1 RefPartners_Units2 (nolock)
	on 
	RefPartners_Units1.NameUnitNew = RefPartners_Units2.NameUnitNew
	and RefPartners_Units1.RowID <> RefPartners_Units2.RowID
	group by RefPartners_Units1.RowID ,RefPartners_Units1.NameUnit,RefPartners_Units1.NameUnitNew;
go


--сверяем с адресатами и подсчитываем количество используемых контрагентов в группе только для контрагентов с нижним подчеркиванием 
with Unit_Duplicate_CTE as (
select 
count(*) coll, t.RUNKROW,t.NameUnit ,t.Rowid,RefPartners_Units.SysRowTimestamp
from RefPartners_Units_temp_2 t 
left join [dbo].[dvtable_{5A296B39-B9F1-406E-9CBC-1123067923C5}] as CardRegistration_Addressees (nolock) on CardRegistration_Addressees.PartnerOrg = t.Rowid 
inner join [dvtable_{C78ABDED-DB1C-4217-AE0D-51A400546923}] RefPartners_Units (nolock) on RefPartners_Units.RowID = t.Rowid
where t.NameUnit like '[_]%'
group by t.RUNKROW,t.NameUnit ,t.Rowid, RefPartners_Units.SysRowTimestamp
)
--записываем в таблицу RefPartners_Units_temp_3 с эталонами (ROWNUM 1) и дублями (ROWNUM остальные)
insert into RefPartners_Units_temp_3
select 
ROW_NUMBER() OVER (partition by RUNKROW order by RUNKROW, coll desc, SysRowTimestamp) ROWNUM
,coll,RUNKROW,Rowid,NameUnit
from Unit_Duplicate_CTE;

go

--сверяем с адресатами и подсчитываем количество используемых контрагентов в группе только для остальных контрагентов 
with Unit_Duplicate_CTE_1 as (
select 
count(*) coll, t.RUNKROW,t.NameUnit ,t.Rowid,RefPartners_Units.SysRowTimestamp
from RefPartners_Units_temp_2 t 
left join [dbo].[dvtable_{5A296B39-B9F1-406E-9CBC-1123067923C5}] as CardRegistration_Addressees (nolock) on CardRegistration_Addressees.PartnerOrg = t.Rowid 
inner join [dvtable_{C78ABDED-DB1C-4217-AE0D-51A400546923}] RefPartners_Units (nolock) on RefPartners_Units.RowID = t.Rowid
where t.NameUnit not like '[_]%'
group by t.RUNKROW,t.NameUnit ,t.Rowid, RefPartners_Units.SysRowTimestamp
)
--записываем в таблицу RefPartners_Units_temp_3 все ROWNUM равны 10
insert into RefPartners_Units_temp_3
select 
10 ROWNUM -- здесь заглушка, всем остальным достается цифра 10 
,coll,RUNKROW,Rowid,NameUnit
from Unit_Duplicate_CTE_1;

go

-- еще раз нумеруем группы и перезаписываем значения по новой
with Unit_Duplicate_CTE_2 as
(select 
ROW_NUMBER() over (partition by RUNKROW order by ROWNUM,COUNTROW desc,NameUnit) ROWNUM,COUNTROW,RUNKROW,RowID,NameUnit
 from RefPartners_Units_temp_3)
 update RefPartners_Units_temp_3  
 set ROWNUM = Unit_Duplicate_CTE_2.ROWNUM 
 FROM Unit_Duplicate_CTE_2 
 where RefPartners_Units_temp_3.RowID = Unit_Duplicate_CTE_2.RowID;
 go
 
--выводим эталоны и дубли ввиде xml (этот запрос просто для выгрузки заказчику)
select cast(t.RowID as nvarchar(max)) + ' | | ' + t.NameUnit as NameUnit,
 (select cast(RowID as nvarchar(max)) + ' | | ' + NameUnit + '#TPFS#'
 from RefPartners_Units_temp_3
 where RUNKROW = t.RUNKROW
 and ROWNUM > '1'
 for xml path('')
 ) as Duplicate
from RefPartners_Units_temp_3 t
where t.ROWNUM = '1'
order by t.RUNKROW
for xml path
go
