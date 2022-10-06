drop database if exists tarjeta_cred;
create database tarjeta_cred;

\c tarjeta_cred

create table cliente(
		nrocliente int, 
		nombre text, 
		apellido text, 
		domicilio text, 
		telefono char(12)
);


create table tarjeta(
	nrotarjeta char(16),
	nrocliente int, 
	validadesde char(6), --e.g. 201106 
	validahasta char(6),
	codseguridad char(4),
	limitecompra decimal(8,2),
	estado char(10) --`vigente', `suspendida', `anulada'
);

create table comercio(
	 nrocomercio int,
	 nombre text, 
	 domicilio text, 
	 codigopostal char(8), 
	 telefono char(12)
);

create table compra(
	nrooperacion SERIAL,
	nrotarjeta char(16), 
	nrocomercio int, 
	fecha timestamp, 
	monto decimal(7,2), 
	pagado boolean
);

create table rechazo(
	nrorechazo serial,
	nrotarjeta char(16), 
	nrocomercio int,
	fecha timestamp, 
	monto decimal(7,2),
	motivo text
);

create table cierre(
	año int,  
	mes int, 
	terminacion int,
	fechainicio date,
	fechacierre date, 
	fechavto date
);

create table cabecera(
	nroresumen int,
	nombre text, 
	apellido text,
	domicilio text,
	nrotarjeta char(16),
	desde date,
	hasta date,
	vence date,
	total decimal(8,2)
);

create table detalle(
	nroresumen int,
	nrolinea int, 
	fecha date, 
	nombrecomercio text,
	monto decimal(7,2)
);

create table alerta(
	nroalerta serial,
	nrotarjeta char(16),
	fecha timestamp,
	nrorechazo int,
	codalerta int, --0:rechazo, 1:compra 1min, 5:compra 5min, 32:límite
	descripcion text
);

create table consumo(
	nrotarjeta char(16),
	codseguridad char(4), 
	nrocomercio int, 
	monto decimal(7,2)
);


alter table cliente add constraint cliente_pk primary key (nrocliente);

alter table tarjeta add constraint tarjeta_pk primary key (nrotarjeta);
alter table tarjeta add constraint tarjeta_fk foreign key (nrocliente) references cliente(nrocliente);

alter table comercio add constraint comercio_pk primary key (nrocomercio);

alter table compra add constraint compra_pk primary key (nrooperacion);
alter table compra add constraint compra_nc_fk foreign key (nrocomercio) references comercio(nrocomercio);
alter table compra add constraint compra_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);

alter table rechazo add constraint rechazo_pk primary key (nrorechazo);
alter table rechazo add constraint rechazo_nc_fk foreign key (nrocomercio) references comercio(nrocomercio);
alter table rechazo add constraint rechazo_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);

alter table cierre add constraint cierre_pk primary key (año, mes, terminacion);

alter table cabecera add constraint cabecera_pk primary key (nroresumen);
alter table cabecera add constraint cabecera_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);


alter table detalle add constraint detalle_pk primary key (nroresumen, nrolinea);
alter table detalle add constraint detalle_nr_fk foreign key (nroresumen) references cabecera(nroresumen);
alter table alerta add constraint alerta_pk primary key (nroalerta);
alter table alerta add constraint alerta_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);
--alter table alerta add constraint alerta_nr_fk foreign key (nrorechazo) references rechazo(nrorechazo);


-- CARGA DE INSTANCIAS

--copy cliente(nrocliente, nombre, apellido, domicilio, telefono) from '/tmp/cliente.dat' delimiter ',' csv header;
--INSERT DE CLIENTES EN LA TABLA DE CLIENTES

insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (256498,'ACEVEDO'	,'ARIEL MATIAS'		,'ASCONAPE 242 Moreno','1164546578');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (225697,'ACOSTA'		,'LAURA RAQUEL'		,'DARDO ROCHA 1168 Paso del Rey','1127643869');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (288763,'BARCALA'	,'PATRICIA EDITH'	,'MERLO 2853 Moreno','1156419452');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (364587,'BERGUIER'	,'NAHUEL'			,'POSADAS 82 Moreno','1126744529');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (456287,'BIANCHI'	,'MARCELA VERÓNICA'	,'ALMIRANTE BROWN 1679 Paso del Rey','1164295275');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (653028,'CAGNOLA'	,'MARIELA LAURA'	,'TUCUMÁN 23  Moreno','1163428529');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (528614,'CALDERON'	,'MARTIN GABRIEL'	,'AVENIDA ITALIA 659 1º General Rodriguez','1124759836');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (358435,'DOMINGUEZ'	,'HERNAN ALEJANDRO'	,'CARLOS PELLEGRINI 728 2º Moreno','1145874292');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (473645,'DOMINGUEZ'	,'NAYLA FLORENCIA'	,'ANTARTIDA ARGENTINA 24 Moreno','1148365419');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (584934,'ECHARRI'	,'MIGUEL ANGEL'		,'POSADAS 82 Moreno','1138419708');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (520763,'EGUIGUREN'	,'LORENA'			,'MERLO 732 Moreno','1152081742');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (276427,'FERNANDEZ'	,'LILIANA ESTELA'	,'BELGRANO 492 Moreno','1164904523');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (384671,'FRANCO'		,'AGUSTIN MARTINEZ'	,'MELO 176 1º Moreno','1134905634');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (439724,'JUÁREZ'		,'MARÍA JOSÉ'		,'TUCUMAN 402 Moreno','1153204500');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (548308,'KARP'		,'GUIDO LEANDRO'	,'MERLO 732 Moreno','175650092');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (386417,'LOMBARDO'	,'MARCELA PAULA'	,'PAGANO 2649 3º 9 Moreno','1163905409');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (397614,'LONGO'		,'PAULO GERMAN'		,'PIERRE BENOIT 2983 Moreno','1145320738');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (478743,'LURO'		,'OSVALDO DANIEL'	,'PADRE VARBELLO 170 PB 0 Paso del Rey','1142674094'); 
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (348240,'MACHADO'	,'EDUARDO DANIEL'	,'NEMESIO ALVAREZ 626 Moreno','1175493502');
insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (453990,'MACIEL'		,'MARÍA CRISTINA'	,'URUGUAY 475 Moreno','1128440093');

-------
--INSERT DE COMERCIOS EN LA TABLA DE COMERCIOS

insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(2365,'Z.VITAL'		,'LIBERTADOR AV. DEL LIBERTADOR 14882 ACASSUSO'		,'B1638BDA','4798-2683');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(5364,'FARMAPLUS'	,'22 AV. SUCRE 2116	BECCAR'							,'B1606DUQ','4737-4282');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(6347,'M & A'		,'AV. SAN MARTIN 404BELLA VISTA'					,'B1661BPM','4668-2607');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(5648,'RADIUM'		,'AV. RICCHIERI 949	BELLA VISTA'					,'B1661BPM','4666-0021');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(4861,'RIAL	'		,'ALVEAR 2571 BENAVIDEZ'							,'B1606DVA','03327-481210');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(6304,'BILLINGHURST','MORENO 4106 BILLINGHURST'							,'B1606ADT','4842-9614');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(5630,'RAMALLO'		,'VILLARROEL PRIMERA JUNTA 5705	BILLINGHURST'		,'B1605BTU','4285-8691');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(5402,'DEL VALLE'	,'AV. AVELINO ROLON 730 BOULOGNE'					,'B1609JWE','4765-4886');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(6487,'SAMBAN'		,'BERNARDO DE IRIGOYEN 601 BOULOGNE'				,'B1609JWE','4737-9946');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(6930,'SANTA RITA'	,'AV. SUCRE 431	BOULOGNE'							,'B1609HUD','4735-2227');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(7943,'DUTKIEWICZ'	,'INDEPENDENCIA 2980 CARAPACHAY'					,'B1607BLB','4762-1168');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(3697,'GIGLIOTTI'	,'AV. SAN MARTIN 2643 CASEROS'						,'B1678GVO','4512-7200');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(2039,'MODERNA'		,'GIGLIOTTI AV. SAN MARTIN 2287	CASEROS'			,'B1678GVO','4512-7204');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(7903,'PAGANINI'	,'PTE. PERON 5599CASEROS'							,'B1678ESB','4751-1022');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(9301,'SEBASTIANI'	,'LISANDRO DE LA TORRE 3497	CASEROS'				,'B1678ESB','4769-5278');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(2307,'GUTKIND'		,'INDEPENDENCIA 7032 DEL VISO'						,'B1666KCD','02320-470171');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(9502,'CASTELLI'	,'AV. A.T. DE ALVEAR 2767 DON TORCUATO'				,'B1660HBD','4748-4350');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(7504,'JKS'			,'AV. A.T. DE ALVEAR 602 DON TORCUATO'				,'B1660HBD','4748-1020');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(8246,'ZONA VITAL'	,'TORCUATO AV. A.T. DE ALVEAR DON TORCUATO'			,'B1660HBD','4748-8500');
insert into comercio (nrocomercio,nombre,domicilio,codigopostal,telefono) values(6314,'CARRILES'	,'AV.MARCONI 1705EL PALOMAR'						,'B1684CYQ','4578-3657');


-------
--INSERT DE TARJETAS EN LA TABLA DE TARJETAS

insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546018597106354',256498,'201803'	,'202103'	,'2569',27500.08, 'vigente'); --EXPIRADA
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4547036803979745',225697,'201802'	,'202108'	,'2986',35000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546369593019648',288763,'201803'	,'202107'	,'5297',45000.78, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4553479503989025',364587,'201804'	,'202108'	,'3941',50000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546456297101369',456287,'201807'	,'202109'	,'9736',75500.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4553212579023694',653028,'201806'	,'202111'	,'8027',25500.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546597630289643',528614,'201705'	,'202207'	,'2704',50000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546365479316325',358435,'201706'	,'202204'	,'2079',70000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4547697579317963',473645,'201708'	,'202207'	,'1907',50000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4547394603973015',584934,'201708'	,'202208'	,'9705',50000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4547393739710287',520763,'201709'	,'202210'	,'9307',65300.08, 'anulada');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546648779646308',276427,'201709'	,'202209'	,'9742',65000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4563539796489987',384671,'201712'	,'202210'	,'9084',35000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546493176233698',439724,'201905'	,'202210'	,'6027',75500.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4547693189643698',548308,'201905'	,'202211'	,'3642',45000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4546694396484518',386417,'201906'	,'202211'	,'6974',55000.00, 'suspendida');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4563039496473369',397614,'201906'	,'202303'	,'9340',45000.00, 'suspendida');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4587493096345698',478743,'201906'	,'202203'	,'7504',75000.00, 'vigente'); 
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4563493769451125',348240,'201910'	,'202304'	,'8046',45000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4556452336978891',453990,'201910'	,'202308'	,'3697',95000.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4575394893973052',584934,'201811'	,'202307'	,'9705',85500.00, 'vigente');
insert into tarjeta (nrotarjeta,nrocliente,validadesde,validahasta,codseguridad,limitecompra,estado) values ('4557456297101896',456287,'201807'	,'202109'	,'9736',75500.00, 'vigente');

-- INSERT DE FECHAS DE CIERRE EN TABLA CIERRE

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 0, '2021-01-01', '2021-01-31', '2021-02-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 1, '2021-01-01', '2021-01-31', '2021-02-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 2, '2021-01-01', '2021-01-31', '2021-02-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 3, '2021-01-01', '2021-01-31', '2021-02-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 4, '2021-01-01', '2021-01-31', '2021-02-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 5, '2021-01-01', '2021-01-31', '2021-02-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 6, '2021-01-01', '2021-01-31', '2021-02-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 7, '2021-01-01', '2021-01-31', '2021-02-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 8, '2021-01-01', '2021-01-31', '2021-02-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 01, 9, '2021-01-01', '2021-01-31', '2021-02-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 0, '2021-02-01', '2021-02-28', '2021-03-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 1, '2021-02-01', '2021-02-28', '2021-03-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 2, '2021-02-01', '2021-02-28', '2021-03-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 3, '2021-02-01', '2021-02-28', '2021-03-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 4, '2021-02-01', '2021-02-28', '2021-03-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 5, '2021-02-01', '2021-02-28', '2021-03-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 6, '2021-02-01', '2021-02-28', '2021-03-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 7, '2021-02-01', '2021-02-28', '2021-03-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 8, '2021-02-01', '2021-02-28', '2021-03-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 02, 9, '2021-02-01', '2021-02-28', '2021-03-19');


insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 0, '2021-03-01', '2021-03-31', '2021-04-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 1, '2021-03-01', '2021-03-31', '2021-04-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 2, '2021-03-01', '2021-03-31', '2021-04-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 3, '2021-03-01', '2021-03-31', '2021-04-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 4, '2021-03-01', '2021-03-31', '2021-04-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 5, '2021-03-01', '2021-03-31', '2021-04-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 6, '2021-03-01', '2021-03-31', '2021-04-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 7, '2021-03-01', '2021-03-31', '2021-04-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 8, '2021-03-01', '2021-03-31', '2021-04-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 03, 9, '2021-03-01', '2021-03-31', '2021-04-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 0, '2021-04-01', '2021-04-30', '2021-05-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 1, '2021-04-01', '2021-04-30', '2021-05-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 2, '2021-04-01', '2021-04-30', '2021-05-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 3, '2021-04-01', '2021-04-30', '2021-05-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 4, '2021-04-01', '2021-04-30', '2021-05-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 5, '2021-04-01', '2021-04-30', '2021-05-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 6, '2021-04-01', '2021-04-30', '2021-05-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 7, '2021-04-01', '2021-04-30', '2021-05-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 8, '2021-04-01', '2021-04-30', '2021-05-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 04, 9, '2021-04-01', '2021-04-30', '2021-05-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 0, '2021-05-01', '2021-05-31', '2021-06-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 1, '2021-05-01', '2021-05-31', '2021-06-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 2, '2021-05-01', '2021-05-31', '2021-06-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 3, '2021-05-01', '2021-05-31', '2021-06-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 4, '2021-05-01', '2021-05-31', '2021-06-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 5, '2021-05-01', '2021-05-31', '2021-06-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 6, '2021-05-01', '2021-05-31', '2021-06-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 7, '2021-05-01', '2021-05-31', '2021-06-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 8, '2021-05-01', '2021-05-31', '2021-06-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 05, 9, '2021-05-01', '2021-05-31', '2021-06-19');





-- INSERT COMPRAS DE UNA TARJETAS VIGENTE, PERO TODAVIA NO PAGADAS
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-12',500.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-12',500.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-13',500.0,false);
--insert into compra (nrooperacion,nrotarjeta,nrocomercio,fecha,monto,pagado) values(6317,'4563493769451125',6314,'2021-03-13',0.0,false);

-- INSERT COMPRAS DE LA MISMA TARJETA VIGENTE ANTERIOR, PERO COMPRAS YA PAGADAS
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-04-12',1500.0,true);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-04-12',1500.0,true);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-04-13',1500.0,true);


-- INSERT DE DOS COMPRAS DE UNA TARJETA QUE LLEGAN JUSTO AL LIMITE (PROXIMA COMPRA DEBERIA RECHAZARSE)
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547036803979745',6314,'2021-05-12',30000.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547036803979745',6314,'2021-05-12',4499.0,false);

-- INSERT EN CONSUMO
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4547036803979745','2986', 7314, 999.00);

---
--Funciones

--SP autorizacion compra

create or replace function autorizacioncompra (nrotarjetab char(16),  codseguridadb char(4),  nrocomercio int, montob decimal(7,2))  returns boolean as $$

 declare
    resultado record;
	codError int;
	autorizacion boolean;
	motivorechazo text;	

begin

     codError :=0;
--	Que el número de tarjeta sea existente, y que corresponda a alguna tarjeta vigente. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?tarjeta no válida ó no vigente.
    select * into resultado from tarjeta where tarjeta.nrotarjeta = nrotarjetab;
    if not found then
        motivorechazo='Tarjeta no valida o vigente';
        codError :=1;
    end if;
    -- deberia actualizar rechazo
  
  --Que el código de seguridad sea el correcto. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?código de seguridad inválido.
    select * into resultado from tarjeta where tarjeta.codseguridad = codseguridadb;
    if not found then
        motivorechazo='El codigo de seguridad es invalido';
         codError :=1;
    end if;
       -- deberia actualizar rechazo
           
  --Que el monto total de compras pendientes de pago más la compra a realizar no supere el límite de compra de la tarjeta. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?supera límite de tarjeta.
     select   *  into resultado   from tarjeta  where  tarjeta.nrotarjeta =  nrotarjetab and limitecompra > 
     (select  
			case  
				when sum(compra.monto)  is null 
					then 0 
					else 
					sum(compra.monto)  
			end 
	 from tarjeta 
    left join compra
    on tarjeta.nrotarjeta = compra.nrotarjeta  where  compra.pagado = false  and  compra.nrotarjeta = nrotarjetab)  +  montob;
   
    if  not found then    
      motivorechazo := 'Limite Excedido';
       codError :=1;
	end if;
        --deberia actualizar rechazo motivo 
        
  --Que la tarjeta no se encuentre vencida. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?plazo de vigencia expirado.
	 select * into resultado from tarjeta where tarjeta.nrotarjeta = nrotarjetab  and  to_date(tarjeta.validahasta, 'YYYYMM')  >  current_date;
    if not found then
       -- raise  notice'la tarjeta se encuentra vencida';
       motivorechazo := 'la tarjeta se encuentra vencida';
       codError :=1;
    end if;
        -- deberia actualizar rechazo motivo		
		
  --Que la tarjeta no se encuentre suspendida. En caso que no cumpla, se debe cargar un rechazo con el mensaje la tarjeta se encuentra suspendida.
    select * into resultado from tarjeta where tarjeta.nrotarjeta = nrotarjetab  and tarjeta.estado != 'suspendida';
    if not found then
        --raise notice 'la tarjeta se encuentra suspendida';
        motivorechazo := 'la tarjeta se encuentra suspendida';
        codError :=1;
    end if;
    
       -- deberia actualizar rechazo	
      if( codError = 0) then
			raise notice 'Consumo Autorizado';
			autorizacion := true;
	else 
	raise notice 'Consumo No Autorizado';
	insert into rechazo (nrotarjeta, nrocomercio, fecha, monto , motivo) values (nrotarjetab, nrocomercio, current_timestamp, montob, motivorechazo);
	autorizacion := false;
	end if;
	return autorizacion;
  end ; 

$$ language plpgsql;




create or replace function generaresumen (nroclienteb int,  periodo int)  returns void as $$

  declare
    resultadoA record;  --resultado de buesqueda en tabla cliente
    resultadoB record;  --resultado de busqueda en tabla tarjeta
    resultadoC record;  --resultado de busqueda en tabla cierre
   	resultadoD  record ;
	rec_count int;
	query  text; 
	i int;
    
    
    digito char(1);     -- extraigo el ultimo digito de la tarjeta
    terminacionb int;
    total decimal(8,2);
    nroresumen int;  --guardo hardcodeado el numero de resumen
    cuenta int;   --acumula cantidad de lineas que va atener el resumen

begin


    select * into resultadoA from cliente where cliente.nrocliente = nroclienteb;
    if not found then
        raise 'el nro de cliente % es inválido', nroclienteb;
    end if;
    raise notice ' resultado A es  % ', resultadoA;

    select * into resultadoB from tarjeta where tarjeta.nrocliente = nroclienteb;
    if not found then
        raise 'tarjeta de cliente % no encontrada', nroclienteb;
    end if;
    
    digito := substring (resultadoB.nrotarjeta from 16);
    terminacionb := cast (digito as integer);
    
    raise notice ' resultado B es  % ', resultadoB;
    raise notice ' Nro tarjeta es   % ', resultadoB.nrotarjeta;
    raise notice ' ultimo digito es %', digito;
 
 
    select * into resultadoC from cierre where cierre.terminacion = terminacionb and cierre.mes = periodo;
    if not found then
        raise 'el nro de tarjeta % es inválida', nrotarjetab;
    end if;

	raise notice ' resultado C es  % ', resultadoC;
	
	select  sum(monto) into total
	from compra
	where compra.nrotarjeta =  resultadoB.nrotarjeta
	and  compra.pagado = false
    and compra.fecha >= resultadoC.fechainicio
	and compra.fecha <=  resultadoC.fechacierre ;
	
	raise notice 'total de consumo %', total;
		
	nroresumen :=25000;   --hardcodeo nroresumen
	
	insert into cabecera( nroresumen, nombre,  apellido, domicilio, nrotarjeta, desde, hasta, vence, total) values(nroresumen, 
	resultadoA.nombre, resultadoA.apellido, resultadoA.domicilio, resultadoB.nrotarjeta, resultadoC.fechainicio, resultadoC.fechacierre,
	resultadoC.fechavto, total);
	
		
	select count(*)  into cuenta
	from compra
	where compra.nrotarjeta =  resultadoB.nrotarjeta
	and  compra.pagado = false
    and compra.fecha >= resultadoC.fechainicio
	and compra.fecha <=  resultadoC.fechacierre ;
	
	raise notice ' cantidad de lineas del resumen:  %', cuenta;
	
	i :=0;							
	for resultadoD  in  select compra.fecha, comercio.nombre, compra.monto from compra
									inner join comercio on compra.nrocomercio = comercio.nrocomercio
									where compra.nrotarjeta =  resultadoB.nrotarjeta
									and  compra.pagado = false
									and compra.fecha >= resultadoC.fechainicio
									and compra.fecha <=  resultadoC.fechacierre 
									limit 10
									
    loop
         i := i +1;
	     raise notice ' %, %, %, %, %' , nroresumen,   i, resultadoD.fecha, resultadoD.nombre, resultadoD.monto     ;
	     insert into detalle( nroresumen, nrolinea,  fecha,  nombrecomercio, monto) values (nroresumen, i,
								resultadoD.fecha, resultadoD.nombre, resultadoD.monto);
	     
	end loop;




end
 
 
 
 

$$ language plpgsql;



select generaresumen(548308, 03);






/*
create or replace function autorizacioncompra()  returns trigger as $$

 declare
    resultado record;
	codError int;
	autorizacion boolean;
	motivorechazo text;	
	parcial record;
	
begin

    codError :=0;
--	Que el número de tarjeta sea existente, y que corresponda a alguna tarjeta vigente. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?tarjeta no válida ó no vigente.
    select * into resultado from tarjeta where tarjeta.nrotarjeta = new.nrotarjeta;
    if not found then
        motivorechazo:= 'el nro de tarjeta % es inválida', new.nrotarjeta;
        codError :=1;
    end if;
    -- deberia actualizar rechazo
  
  --Que el código de seguridad sea el correcto. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?código de seguridad inválido.
    select * into resultado from tarjeta where tarjeta.codseguridad = new.codseguridad;
    if not found then
        motivorechazo := 'el nro de codseguridad % es inválido', new.codseguridad;
         codError :=1;
    end if;
       -- deberia actualizar rechazo
           
  --Que el monto total de compras pendientes de pago más la compra a realizar no supere el límite de compra de la tarjeta. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?supera límite de tarjeta.
     select   *  into resultado   from tarjeta  where  tarjeta.nrotarjeta =  new.nrotarjeta and limitecompra > 
     (select  
			case  
				when sum(compra.monto)  is null 
					then 0 
					else 
					sum(compra.monto)  
			end 
	 from tarjeta 
    left join compra
    on tarjeta.nrotarjeta = compra.nrotarjeta  where  compra.pagado = false  and  compra.nrotarjeta = new.nrotarjeta)  +  new.monto;
   
    if  not found then    
      --raise notice 'gastaste mucho ';
      motivorechazo := 'Limite Excedido';
       codError :=1;
	end if;
        --deberia actualizar rechazo motivo 
        
  --Que la tarjeta no se encuentre vencida. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?plazo de vigencia expirado.
	 select * into resultado from tarjeta where tarjeta.nrotarjeta = new.nrotarjeta  and  to_date(tarjeta.validahasta, 'YYYYMM')  >  current_date;
    if not found then
       -- raise  notice'la tarjeta se encuentra vencida';
       motivorechazo := 'la tarjeta se encuentra vencida';
         codError :=1;
    end if;
        -- deberia actualizar rechazo motivo		
		
  --Que la tarjeta no se encuentre suspendida. En caso que no cumpla, se debe cargar un rechazo con el mensaje la tarjeta se encuentra suspendida.
    select * into resultado from tarjeta where tarjeta.nrotarjeta = new.nrotarjeta  and tarjeta.estado != 'suspendida';
    if not found then
        --raise notice 'la tarjeta se encuentra suspendida';
        motivorechazo := 'la tarjeta se encuentra suspendida';
         codError :=1;
    end if;
    
       -- deberia actualizar rechazo	
      if( codError = 0) then
			raise notice 'Consumo Autorizado';
			
			insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values (new.nrotarjeta, new.nrocomercio, current_date, new.monto, false);
			autorizacion := true;
	else 
	raise notice 'Consumo No Autorizado';
	intento transaccion fumeta sin ver el video
	begin
	insert into rechazo (nrotarjeta, nrocomercio, fecha, monto ,  motivo) values (new.nrotarjeta, new.nrocomercio, current_date, new.monto, motivorechazo);
	autorizacion := false;
	commit;
	begin
	select max (nrorechazo) into parcial from rechazo;
	insert into alerta (nroalerta, nrotarjeta, fecha, nrorechazo, codalerta, descripcion) values ( new.nrotarjeta, current_date, parcial.nrorechazo, 0, motivorechazo);
	commit;
	insert into rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo) values (new.nrotarjeta, new.nrocomercio, current_date, new.monto, motivorechazo);
	end if;
	return new;
  end ; 

$$ language plpgsql;

create trigger autorizacioncompra_trg 
before insert on consumo
for each row
execute procedure autorizacioncompra() ;
*/


--Todo rechazo se debe ingresar automáticamente a la tabla de alertas. No puede haber ninguna demora para ingresar (Trigger sobre rechazo)
-- un rechazo en la tabla de alertas, se debe ingresar en el mismo instante en que se generó el rechazo.

create or replace function nuevorechazo()  returns trigger as $$

	
begin
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_date, new.nrorechazo, 0, new.motivo);
	return new;
 end ; 

$$ language plpgsql;

create trigger nuevorechazo_trg 
after insert on rechazo
for each row
execute procedure nuevorechazo() ;


--Si una tarjeta registra dos compras en un lapso menor de un minuto en comercios distintos ubicados en el mismo código postal.

create or replace function comprasconsecutivas()  returns trigger as $$

 declare
    ultimacompra record;
	ultimocomerciocp char(8);
	nuevocomerciocp char(8);
	
begin
	
	select * into ultimacompra from compra where nrotarjeta = new.nrotarjeta order by nrooperacion desc limit 1 ;
	
	select codigopostal into ultimocomerciocp from comercio where nrocomercio = ultimacompra.nrocomercio;
	select codigopostal into nuevocomerciocp from comercio where nrocomercio = new.nrocomercio;
	
	
	if (new.nrocomercio!=ultimacompra.nrocomercio
	and ultimocomerciocp=nuevocomerciocp
	and (select date_trunc('hour',new.fecha)) = (select date_trunc('hour',ultimacompra.fecha))
	and  (select extract(minute from (select age (new.fecha,ultimacompra.fecha))) < 1)) 
	then
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_timestamp, new.nrorechazo, 1, '2 compras en menos de 1 minuto');
	end if;
	return new;
 end ; 

$$ language plpgsql;

create trigger comprasconsecutivas_trg 
before insert on compra
for each row
execute procedure comprasconsecutivas() ;


--Si una tarjeta registra dos compras en un lapso menor de 5 minutos en comercios con diferentes códigos postales.

create or replace function comprasmenorcinco()  returns trigger as $$

 declare
    ultimacompra record;
	ultimocomerciocp char(8);
	nuevocomerciocp char(8);
	
begin
	select * into ultimacompra from compra where nrotarjeta = new.nrotarjeta order by nrooperacion desc limit 1 ;
	
	select codigopostal into ultimocomerciocp from comercio where nrocomercio = ultimacompra.nrocomercio;
	select codigopostal into nuevocomerciocp from comercio where nrocomercio = new.nrocomercio;
	
	if (ultimocomerciocp!=nuevocomerciocp
	and (select date_trunc('hour',new.fecha)) = (select date_trunc('hour',ultimacompra.fecha))
	and  (select extract(minute from (select age (new.fecha,ultimacompra.fecha))) < 5)) 
	then
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_timestamp, new.nrooperacion, 1, '2 compras en menos de 5 minuto con diferente CP');
	end if;
	return new;

 end ; 

$$ language plpgsql;

create trigger comprasmenorcinco_trg 
before insert on compra
for each row
execute procedure comprasmenorcinco() ;


--Si una tarjeta registra 2 rechazos por exceso de limite en el mismo dia , la tarjeta tiene que ser suspendida preventivamente
--y se debe guardar una alerta con el cambio de estado.

create or replace function dosrechazosmismodia()  returns trigger as $$

declare

anteultimorechazo record;

begin

select * into anteultimorechazo from rechazo  where  nrotarjeta=new.nrotarjeta and rechazo.motivo= 'Limite Excedido' order by nrorechazo desc limit 1;
--raise notice 'la tarjeta se encuentra suspendida',anteultimorechazo;
if ( anteultimorechazo.fecha = new.fecha and anteultimorechazo.motivo = new.motivo)  then

update tarjeta set  estado = 'suspendido' where tarjeta.nrotarjeta = new.nrotarjeta;

insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) 
values (new.nrotarjeta, current_timestamp, new.nrorechazo, 0, 'Tarjeta suspendida por falta de presupuesto.');
end if;
return new;
 end ; 
 
 $$ language plpgsql;
 
 
create trigger dosrechazosmismodia_trg 
before insert on rechazo
for each row
execute procedure dosrechazosmismodia();

--Si una tarjeta registra dos rechazos por exceso de límite en el mismo día, la tarjeta tiene que ser suspendida preventivamente, 
--y se debe grabar una alerta asociada a este cambio de estado.

/*create or replace function nuevorechazo()  returns trigger as $$
	
	ultimorecord record;
	
begin
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_date, new.nrorechazo, 0, new.motivo);
	return new;
 end ; 

$$ language plpgsql;

create trigger nuevorechazo_trg 
after insert on rechazo
for each row
execute procedure nuevorechazo() ;
*/





/*create or replace function nrocomercio(nrocomercio int)  returns void as $$

begin
 raise  notice 'nro nrocomercio  es el mejor:  % '  , nrocomercio;
 end ; 

$$ language plpgsql;*/





-- CASOS DE PRUEBA

--select autorizacioncompra ( '4563493769451125',  '8046',  6314, 2500.50);    --Ejemplo con tarjeta vigente y activa.  "resultado: consumo autorizado"

--select autorizacioncompra ( '4547693189643698',  '3642',  6314, 2500.50);    --Ejemplo con tarjeta vigente y activa y compras anteriores. "resultado: consumo autorizado"

--select autorizacioncompra ( '4563493769451125',  '8146',  6314, 2500.50);    --Ejemplo con tarjeta vigente y activa y codigo seguridad incorrecto "resultado: codigo seguridad incorrecto "

--select autorizacioncompra ('4546018597106354',  '2569',  6314, 2500.50);    --Ejemplo con tarjeta expirada "rtesultado; la tarjeta se encuentra vencida"

--select autorizacioncompra ('4547036803979745',  '2986',  6314, 1000.50);    --Ejemplo con tarjeta que sobrepasa el limite de compra


/*

4	Stored Procedures y Triggers
El trabajo práctico deberá incluir los siguientes stored procedures ó triggers:
•	autorización de compra se deberá incluir la lógica que reciba los datos de una compra—número de tarjeta, código de seguridad, número de comercio y monto—y que devuelva true si se autoriza la compra ó false si se rechaza. El procedimiento deberá validar los siguientes elementos antes de autorizar:
		–	Que el número de tarjeta sea existente, y que corresponda a alguna tarjeta vigente. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?tarjeta no válida ó no vigente.
		–	Que el código de seguridad sea el correcto. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?código de seguridad inválido.
		–	Que el monto total de compras pendientes de pago más la compra a realizar no supere el límite de compra de la tarjeta. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?supera límite de tarjeta.
		–	Que la tarjeta no se encuentre vencida. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?plazo de vigencia expirado.
		–	Que la tarjeta no se encuentre suspendida. En caso que no cumpla, se debe cargar un rechazo con el mensaje la tarjeta se encuentra suspendida.
		Si se aprueba la compra, se deberá guardar una fila en la tabla compra, con los datos de la compra.

•	generación del resumen el trabajo práctico deberá contener la lógica que reciba como parámetros el número de cliente, 
y el periodo del año, y que guarde en las tablas que corresponda los datos del resumen con la siguiente información: nombre  y apellido, dirección, número de tarjeta, periodo del resumen, fecha de vencimiento, todas las compras del periodo, y total a pagar.

•	alertas a clientes el trabajo práctico deberá proveer la lógica que genere alertas por posibles fraudes. Existe un Call Centre 
que ante cada alerta generada automática- mente, realiza un llamado telefónico a le cliente, indicándole la alerta detectada, 
y verifica si se trató de un fraude ó no. Se supone que la detección de alertas se ejecuta automáticamente con cierta frecuencia
—e.g. de una vez por minuto. Se pide detectar y almacenar las siguientes alertas:
		–	Todo rechazo se debe ingresar automáticamente a la tabla de alertas. No puede haber ninguna demora para ingresar (Trigger sobre rechazo)
		un rechazo en la tabla de alertas, se debe ingresar en el mismo instante en que se generó el rechazo.
		–	Si una tarjeta registra dos compras en un lapso menor de un minuto en comercios distintos ubicados en el mismo código postal.
		–	Si una tarjeta registra dos compras en un lapso menor de 5 minutos en comercios con diferentes códigos postales.
		–	Si una tarjeta registra dos rechazos por exceso de límite en el mismo día, la tarjeta tiene que ser suspendida preventivamente, 
		y se debe grabar una alerta asociada a este cambio de estado.

Se deberá crear una tabla con consumos virtuales para probar el sistema, la misma deberá contener los atributos: nrotarjeta, codseguridad, nrocomercio, monto. Y se deberá hacer un procedimiento de testeo, que pida autorización para todos los consumos virtuales.
Todo el código SQL escrito para este trabajo práctico, deberá poder ejecutarse desde una aplicación CLI escrita en Go.}

*/
