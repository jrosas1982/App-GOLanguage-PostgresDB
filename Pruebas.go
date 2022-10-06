package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	bolt "github.com/coreos/bbolt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strconv"
	"time"
)

func main() {
	menu()
}

type consumo struct {
	nrotarjeta, codseguridad string
	nrocomercio              int
	monto                    float32
}

type resumen struct {
	nrotarjeta string
	nombre     string
	apellido   string
	domicilio  string
	desde      string
	hasta      string
	vence      string
	total      float32
}

type Cliente struct {
	Nrocliente int
	Nombre     string
	Apellido   string
	Domicilio  string
	Telefono   string
}

type Tarjeta struct {
	Nrotarjeta   string
	Nrocliente   int
	Validadesde  string
	Validahasta  string
	Codseguridad string
	Limitecompra float64
	Estado       string
}

type Comercio struct {
	Nrocomercio  int
	Nombre       string
	Domicilio    string
	Codigopostal string
	Telefono     string
}

type Compra struct {
	Nrooperacion int
	Nrotarjeta   string
	Nrocomercio  int
	Fecha        string
	Monto        float64
	Pagado       bool
}

func menu() {
	var option int
	fmt.Printf("MENÚ\n")
	fmt.Printf("=======================\n")
	fmt.Printf("\n")
	fmt.Printf("1 - Crear Base de Datos SQL\n")
	fmt.Printf("2 - Crear Tablas\n")
	fmt.Printf("3 - Crear PK'S y FK'S a las tablas\n")
	fmt.Printf("4 - Activar Stored Procedures y Triggers\n")
	fmt.Printf("5 - Cargar instancia de datos\n")
	fmt.Printf("6 - Simular transacciones\n")
	fmt.Printf("7 - Generar resumen de compras\n")
	fmt.Printf("8 - Remover PK'S y FK'S de las tablas\n")
	fmt.Printf("9 - Crear y cargar datos en Base NOSQL\n")
	fmt.Printf("0 - Cerrar Menu\n")
	fmt.Printf("\n")
	fmt.Printf("Elija una opción: ")
	fmt.Scanf("%d", &option)
	execute(option)
}

func execute(opt int) {
	switch opt {
	case 1:
		CreateDatabase()
		fmt.Printf("Opción %d : CREADA ---- Base de datos: dbcard\n", opt)
	case 2:
		CreateTables()
		fmt.Printf("Opción %d : CREADAS ---- Tablas\n", opt)
	case 3:
		AddPkAndFk()
		fmt.Printf("Opción %d : CREADAS ----  Claves primarias y foráneas\n", opt)
	case 4:
		ActivarSPandTriggers()
		fmt.Printf("Opción %d : ACTIVADOS ---- Stored Procedures y Triggers\n", opt)
	case 5:
		AddData()
		fmt.Printf("Opción %d : CARGADA ---- Instancia de datos\n", opt)
	case 6:
		VirtualBuys()
		fmt.Printf("Opción %d : CARGADAS ----  Transacciones virtuales realizadas\n", opt)
	case 7:
		SummarySales()
		fmt.Printf("Opción %d : CREADO ---- Resumen de tarjeta \n", opt)
	case 8:
		RemovePkAndFk()
		fmt.Printf("Opción %d : BORRADAS ----  Claves primarias y foráneas\n", opt)
	case 9:
		AddJSONData()
		fmt.Printf("Opción %d : CREADA ---- Base de datos  NOSQL con datos solicitados\n", opt)
	case 0:
		fmt.Printf("Opción %d : CERRANDO....\n", opt)
		terminarMenu()
	default:
		fmt.Printf("La opción %d no ejecuta ninguna acciòn vuelva a ingrear una opciòn valida: ", opt)
	}
	time.Sleep(1 * time.Second)
	fmt.Printf("\n\n")
	main()
}

//============================================================
//										SQL
//============================================================

func CreateDatabase() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`drop database if exists dbcard`)
	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`create database dbcard`)
	if err != nil {
		log.Fatal(err)
	}
}

func CreateTables() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create table cliente(
    nrocliente int,
    apellido text,
    nombre text,
    domicilio text,
    telefono char(12));`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table tarjeta(
    nrotarjeta char(16),
    nrocliente int,
    validadesde char(6),
    validahasta char(6),
    codseguridad char(4),
    limitecompra decimal(8,2),
    estado char(10));`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table comercio(
	 nrocomercio int,
	 nombre text,
	 domicilio text,
	 codigopostal char(8),
	 telefono char(12));`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table compra(
	nrooperacion SERIAL,
	nrotarjeta char(16),
	nrocomercio int,
	fecha timestamp,
	monto decimal(7,2),
	pagado boolean
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table rechazo(
	nrorechazo serial,
	nrotarjeta char(16),
	nrocomercio int,
	fecha timestamp,
	monto decimal(7,2),
	motivo text
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table cierre(
	año int,
	mes int,
	terminacion int,
	fechainicio date,
	fechacierre date,
	fechavto date
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table cabecera(
	nroresumen serial,
	nombre text,
	apellido text,
	domicilio text,
	nrotarjeta char(16),
	desde date,
	hasta date,
	vence date,
	total decimal(8,2)
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table detalle(
	nroresumen int,
	nrolinea int,
	fecha date,
	nombrecomercio text,
	monto decimal(7,2)
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table alerta(
	nroalerta serial,
	nrotarjeta char(16),
	fecha timestamp,
	nrorechazo int,
	codalerta int, --0:rechazo, 1:compra 1min, 5:compra 5min, 32:límite
	descripcion text
);`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = db.Exec(`create table consumo(
	nrotarjeta char(16),
	codseguridad char(4), 
	nrocomercio int, 	
	monto decimal(7,2)
);
`)
	if err != nil {
		log.Fatal(err)
	}

}

func AddPkAndFk() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`alter table cliente add constraint cliente_pk primary key (nrocliente);

  alter table tarjeta add constraint tarjeta_pk primary key (nrotarjeta);
  alter table tarjeta add constraint tarjeta_fk foreign key (nrocliente) references cliente(nrocliente);

  alter table comercio add constraint comercio_pk primary key (nrocomercio);

  alter table compra add constraint compra_pk primary key (nrooperacion);
  alter table compra add constraint compra_nc_fk foreign key (nrocomercio) references comercio(nrocomercio);
  alter table compra add constraint compra_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);

  alter table rechazo add constraint rechazo_pk primary key (nrorechazo);
  alter table rechazo add constraint rechazo_nc_fk foreign key (nrocomercio) references comercio(nrocomercio);
  
  alter table cierre add constraint cierre_pk primary key (año, mes, terminacion);

  alter table cabecera add constraint cabecera_pk primary key (nroresumen);
  alter table cabecera add constraint cabecera_nt_fk foreign key (nrotarjeta) references tarjeta(nrotarjeta);

  alter table detalle add constraint detalle_pk primary key (nroresumen, nrolinea);
  alter table detalle add constraint detalle_nr_fk foreign key (nroresumen) references cabecera(nroresumen);
  
  alter table alerta add constraint alerta_pk primary key (nroalerta);
`)
	if err != nil {
		log.Fatal(err)
	}

}

func ActivarSPandTriggers() {

	StoredProcedures()
	triggeralertadosrechazos()
	alertanuevorechazo()
	comprasconsecutivasalerta()
	comprasconsecutivasalertacincominutos()

}

func RemovePkAndFk() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`alter table cliente drop constraint cliente_pk cascade;

  alter table tarjeta drop constraint tarjeta_pk cascade;
  
  alter table comercio drop constraint comercio_pk cascade;
  
  alter table compra drop constraint compra_pk cascade;
  
  alter table rechazo drop constraint rechazo_pk cascade;
  
  alter table cierre drop constraint cierre_pk cascade;
   
  alter table cabecera drop constraint cabecera_pk cascade;
  
  alter table detalle drop constraint detalle_pk cascade;
  
  alter table alerta drop constraint alerta_pk cascade;
   
  `)
	if err != nil {
		log.Fatal(err)
	}
}

func AddData() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`insert into cliente (nrocliente,nombre,apellido,domicilio,telefono)	values (256498,'ACEVEDO'	,'ARIEL MATIAS'		,'ASCONAPE 242 Moreno','1164546578');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (225697,'ACOSTA'		,'LAURA RAQUEL'		,'DARDO ROCHA 1168 Paso del Rey','1127643869');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (288763,'BARCALA'	,'PATRICIA EDITH'	,'MERLO 2853 Moreno','1156419452');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (364587,'BERGUIER'	,'NAHUEL'			,'POSADAS 82 Moreno','1126744529');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (456287,'BIANCHI'	,'MARCELA VERÓNICA'	,'ALMIRANTE BROWN 1679 Paso del Rey','1164295275');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (653028,'CAGNOLA'	,'MARIELA LAURA'	,'TUCUMÁN 23  Moreno','1163428529');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (528614,'CALDERON'	,'MARTIN GABRIEL'	,'AVENIDA ITALIA 659 1º General Rodriguez','1124759836');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (358435,'DOMINGUEZ'	,'HERNAN ALEJANDRO'	,'CARLOS PELLEGRINI 728 2º Moreno','1145874292');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (473645,'DOMINGUEZ'	,'NAYLA FLORENCIA'	,'ANTARTIDA ARGENTINA 24 Moreno','1148365419');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (584934,'ECHARRI'	,'MIGUEL ANGEL'		,'POSADAS 82 Moreno','1138419708');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (520763,'EGUIGUREN'	,'LORENA'			,'MERLO 732 Moreno','1152081742');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (276427,'FERNANDEZ'	,'LILIANA ESTELA'	,'BELGRaño 492 Moreno','1164904523');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (384671,'FRANCO'		,'AGUSTIN MARTINEZ'	,'MELO 176 1º Moreno','1134905634');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (439724,'JUÁREZ'		,'MARÍA JOSÉ'		,'TUCUMAN 402 Moreno','1153204500');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (548308,'KARP'		,'GUIDO LEANDRO'	,'MERLO 732 Moreno','1756500921');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (386417,'LOMBARDO'	,'MARCELA PAULA'	,'PAGaño 2649 3º 9 Moreno','1163905409');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (397614,'LONGO'		,'PAULO GERMAN'		,'PIERRE BENOIT 2983 Moreno','1145320738');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (478743,'LURO'		,'OSVALDO DANIEL'	,'PADRE VARBELLO 170 PB 0 Paso del Rey','1142674094');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (348240,'MACHADO'	,'EDUARDO DANIEL'	,'NEMESIO ALVAREZ 626 Moreno','1175493502');
insert into cliente (nrocliente,apellido,nombre,domicilio,telefono)	values (453990,'MACIEL'		,'MARÍA CRISTINA'	,'URUGUAY 475 Moreno','1128440093');

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

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 0, '2021-06-01', '2021-06-30', '2021-07-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 1, '2021-06-01', '2021-06-30', '2021-07-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 2, '2021-06-01', '2021-06-30', '2021-07-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 3, '2021-06-01', '2021-06-30', '2021-07-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 4, '2021-06-01', '2021-06-30', '2021-07-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 5, '2021-06-01', '2021-06-30', '2021-07-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 6, '2021-06-01', '2021-06-30', '2021-07-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 7, '2021-06-01', '2021-06-30', '2021-07-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 8, '2021-06-01', '2021-06-30', '2021-07-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 06, 9, '2021-06-01', '2021-06-30', '2021-07-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 0, '2021-07-01', '2021-07-31', '2021-08-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 1, '2021-07-01', '2021-07-31', '2021-08-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 2, '2021-07-01', '2021-07-31', '2021-08-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 3, '2021-07-01', '2021-07-31', '2021-08-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 4, '2021-07-01', '2021-07-31', '2021-08-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 5, '2021-07-01', '2021-07-31', '2021-08-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 6, '2021-07-01', '2021-07-31', '2021-08-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 7, '2021-07-01', '2021-07-31', '2021-08-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 8, '2021-07-01', '2021-07-31', '2021-08-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 07, 9, '2021-07-01', '2021-07-31', '2021-08-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 0, '2021-08-01', '2021-08-31', '2021-09-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 1, '2021-08-01', '2021-08-31', '2021-09-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 2, '2021-08-01', '2021-08-31', '2021-09-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 3, '2021-08-01', '2021-08-31', '2021-09-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 4, '2021-08-01', '2021-08-31', '2021-09-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 5, '2021-08-01', '2021-08-31', '2021-09-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 6, '2021-08-01', '2021-08-31', '2021-09-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 7, '2021-08-01', '2021-08-31', '2021-09-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 8, '2021-08-01', '2021-08-31', '2021-09-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 08, 9, '2021-08-01', '2021-08-31', '2021-09-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 0, '2021-09-01', '2021-09-30', '2021-10-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 1, '2021-09-01', '2021-09-30', '2021-10-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 2, '2021-09-01', '2021-09-30', '2021-10-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 3, '2021-09-01', '2021-09-30', '2021-10-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 4, '2021-09-01', '2021-09-30', '2021-10-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 5, '2021-09-01', '2021-09-30', '2021-10-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 6, '2021-09-01', '2021-09-30', '2021-10-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 7, '2021-09-01', '2021-09-30', '2021-10-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 8, '2021-09-01', '2021-09-30', '2021-10-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 09, 9, '2021-09-01', '2021-09-30', '2021-10-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 0, '2021-10-01', '2021-10-31', '2021-11-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 1, '2021-10-01', '2021-10-31', '2021-11-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 2, '2021-10-01', '2021-10-31', '2021-11-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 3, '2021-10-01', '2021-10-31', '2021-11-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 4, '2021-10-01', '2021-10-31', '2021-11-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 5, '2021-10-01', '2021-10-31', '2021-11-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 6, '2021-10-01', '2021-10-31', '2021-11-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 7, '2021-10-01', '2021-10-31', '2021-11-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 8, '2021-10-01', '2021-10-31', '2021-11-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 10, 9, '2021-10-01', '2021-10-31', '2021-11-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 0, '2021-11-01', '2021-11-30', '2021-12-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 1, '2021-11-01', '2021-11-30', '2021-12-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 2, '2021-11-01', '2021-11-30', '2021-12-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 3, '2021-11-01', '2021-11-30', '2021-12-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 4, '2021-11-01', '2021-11-30', '2021-12-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 5, '2021-11-01', '2021-11-30', '2021-12-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 6, '2021-11-01', '2021-11-30', '2021-12-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 7, '2021-11-01', '2021-11-30', '2021-12-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 8, '2021-11-01', '2021-11-30', '2021-12-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 11, 9, '2021-11-01', '2021-11-30', '2021-12-19');

insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 0, '2021-12-01', '2021-12-31', '2022-01-10');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 1, '2021-12-01', '2021-12-31', '2022-01-11');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 2, '2021-12-01', '2021-12-31', '2022-01-12');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 3, '2021-12-01', '2021-12-31', '2022-01-13');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 4, '2021-12-01', '2021-12-31', '2022-01-14');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 5, '2021-12-01', '2021-12-31', '2022-01-15');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 6, '2021-12-01', '2021-12-31', '2022-01-16');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 7, '2021-12-01', '2021-12-31', '2022-01-17');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 8, '2021-12-01', '2021-12-31', '2022-01-18');
insert into  cierre(año ,  mes , terminacion ,fechainicio ,fechacierre,  fechavto ) values (2021, 12, 9, '2021-12-01', '2021-12-31', '2022-01-19');


-- INSERT COMPRAS DE UNA TARJETAS VIGENTE, PERO TODAVIA NO PAGADAS
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-12',500.0,false); --cliente  548308 
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-12',500.0,false); 
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-03-13',500.0,false);

insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4546369593019648',8246,'2021-04-12',600.0,false); --cliente 288763
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4546369593019648',8246,'2021-04-13',600.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4546369593019648',8246,'2021-04-20',600.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4546369593019648',8246,'2021-04-21',600.0,false);
																															
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4553479503989025',8246,'2021-05-16',700.0,false); --cliente 364587
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4553479503989025',8246,'2021-05-16',700.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4553479503989025',8246,'2021-05-19',700.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4553479503989025',8246,'2021-05-21',700.0,false);


-- INSERT COMPRAS DE LA MISMA TARJETA VIGENTE ANTERIOR, PERO COMPRAS YA PAGADAS
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-02-12',1500.0,true); --cliente  548308 
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-02-12',1500.0,true);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547693189643698',6314,'2021-02-13',1500.0,true);

-- INSERT DE DOS COMPRAS DE UNA TARJETA QUE LLEGAN JUSTO AL LIMITE (PROXIMA COMPRA DEBERIA RECHAZARSE)
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547036803979745',6314,'2021-05-12',30000.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547036803979745',6314,'2021-05-12',4499.0,false);

-- INSERT DOS RECHAZOS POR EXCESO DE LIMITE EN EL MISMO DIA

insert into rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo) values  ( '4553479503989025', 5648, '2021-06-05', 51000.0, 'supera limite de tarjeta');
insert into rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo) values  ( '4553479503989025', 6314, '2021-06-05', 55000.0, 'supera limite de tarjeta');


-- INSERT EN CONSUMO DE OPERACIONES NORMALES
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4547693189643698',  '3642',  6314, 2500.50);


-- INSERT DE CONSUMO DE UNA TARJETA EXPIRADA
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4546018597106354',  '2569',  6314, 2500.50); 


-- INSERT DE CONSUMO QUE SUPERA EL LIMITE DISPONIBLE YA QUE HAY DOS COMPRAS ANTERIORES
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4547036803979745',  '2986',  6314, 1000.50); 

-- INSERT DOS CONSUMOS  DE LA MISMA TARJETA  EN  DOS CODIGOS POSTALES DISTINTOS
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4563493769451125',  '6347',  2365, 5500.50);
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4563493769451125',  '5648',  5364, 1500.50); 

--INSERT TARJETA NO VALIDA
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('1111111111111111',  '8146',  5364, 1500.50); 

--INSERT TARJETA SUSPENDIDA
insert into consumo (nrotarjeta, codseguridad , nrocomercio , monto) values ('4546694396484518', '6974', 5364, 1500.50); 


--INSERT 2 COMPRAS EN 5 MINUTOS DISTINTO CODIGO POSTAL
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547394603973015',6314,'2021-05-12 17:00:01',100.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547394603973015',8246,'2021-05-12 17:03:03',200.0,false);

--INSERT 2 COMPRAS EN MENOS DE 1 MINUTOS MISMO CODIGO POSTAL
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547394603973015',6347,'2021-05-12 17:00:05',100.0,false);
insert into compra (nrotarjeta,nrocomercio,fecha,monto,pagado) values('4547394603973015',5648,'2021-05-12 17:01:03',200.0,false);



`)





	if err != nil {
		log.Fatal(err)
	}
}

func VirtualBuys() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	rows, err := db.Query(`select * from consumo`)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var a consumo

	for rows.Next() {
		if err := rows.Scan(&a.nrotarjeta, &a.codseguridad, &a.nrocomercio, &a.monto); err != nil {
			log.Fatal(err)
		}
		var autorizacioncompra bool
		sqlStatement1 := `select  autorizacioncompra($1, $2, $3, $4 )`
		err := db.QueryRow(sqlStatement1, a.nrotarjeta, a.codseguridad, a.nrocomercio, a.monto).Scan(&autorizacioncompra)
		if err != nil {
			log.Fatal(err)
		}
		if autorizacioncompra == true {
			sqlStatement := ` INSERT INTO compra (nrotarjeta, nrocomercio, fecha, monto, pagado)
				VALUES ($1, $2, $3, $4, $5)`
			_, err = db.Exec(sqlStatement, a.nrotarjeta, a.nrocomercio, time.Now(), a.monto, false)
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Printf(" Se registro la compra exitosamente >> %v %v %v %v\n", a.nrotarjeta, a.codseguridad, a.nrocomercio, a.monto)
		} else {
			//fmt.Printf(" Error:  Intente mas tarde >> %v %v %v %v\n", a.nrotarjeta, a.codseguridad, a.nrocomercio, a.monto)
		}
	}
	if err = rows.Err(); err != nil {
		log.Fatal(err)
	}

}

func SummarySales() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	sqlStatement2 := `  select  generaresumen($1, $2)`
	rows, err := db.Query(sqlStatement2, 288763, 04)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

}

func StoredProcedures() {

	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create or replace function autorizacioncompra (nrotarjetab char(16),  codseguridadb char(4),  nrocomercio int, montob decimal(7,2))  returns boolean as $$

 declare
    resultado record;
	codError int;
	autorizacion boolean;
	motivorechazo text;	

begin

     codError :=0;

  
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
      motivorechazo := 'supera limite de tarjeta';
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
    
   --	Que el número de tarjeta sea existente, y que corresponda a alguna tarjeta vigente. En caso de que no cumpla, se debe cargar un rechazo con el mensaje ?tarjeta no válida ó no vigente.
    select * into resultado from tarjeta where tarjeta.nrotarjeta = nrotarjetab;
    if not found then
        motivorechazo='Tarjeta no valida';
        codError :=1;
    end if;
    -- deberia actualizar rechazo
          
       -- deberia actualizar rechazo	
      if( codError = 0) then
			autorizacion := true;
	else 
	insert into rechazo (nrotarjeta, nrocomercio, fecha, monto , motivo) values (nrotarjetab, nrocomercio, current_timestamp, montob, motivorechazo);
	autorizacion := false;
	end if;
	return autorizacion;
  end ; 

$$ language plpgsql;`)

	if err != nil {
		log.Fatal(err)
	}

	_, err = db.Exec(`   create or replace function generaresumen (nroclienteb int,  periodo int)  returns void as $$

	declare
    resultadoA record;  	--resultado de buesqueda en tabla cliente
    resultadoB record;  	--resultado de busqueda en tabla tarjeta
    resultadoC record;  	--resultado de busqueda en tabla cierre
   	resultadoD  record ; 	-- info para insertar en tabla  detalle
	i int;                             	--  variable para un contador
        
    digito char(1);     			-- extraigo el ultimo digito de la tarjeta
    terminacionb int;  		-- casteo la variable anterior a int 
    total decimal(8,2);  	-- acumulo el total de la compra
    nroresumen int;  --guardo hardcodeado el numero de resumen
    cuenta int;   --acumula cantidad de lineas que va atener el resumen

	begin

--	LEVANTO LOS DATOS DEL CLIENTE DE TABLA CLIENTE
    select * into resultadoA from cliente where cliente.nrocliente = nroclienteb;
    if not found then
        raise 'el nro de cliente % es inválido', nroclienteb;
    end if;

--	LEVANTO NUMERO  DE LA TARJETA DE TABLA TARJETA
    select * into resultadoB from tarjeta where tarjeta.nrocliente = nroclienteb;
    if not found then
        raise 'tarjeta de cliente % no encontrada', nroclienteb;
    end if;
    
    digito := substring (resultadoB.nrotarjeta from 16);
    terminacionb := cast (digito as integer);
    
--	LEVANTO LOS DATOS DEL CIERRE DE TARJETA  DE TABLA CIERRE   
    select * into resultadoC from cierre where cierre.terminacion = terminacionb and cierre.mes = periodo;
    if not found then
        raise 'el nro de tarjeta % es inválida', nrotarjetab;
    end if;

--	SUMO LOS IMPORTES DE CADA COMPRA EN EL PERIODO SOLICITADO	
	select  sum(monto) into total
	from compra
	where compra.nrotarjeta =  resultadoB.nrotarjeta
	and  compra.pagado = false
    and compra.fecha >= resultadoC.fechainicio
	and compra.fecha <=  resultadoC.fechacierre ;
	

	nroresumen :=25000;   --hardcodeo nroresumen

--	INSERTO LOS DATOS EN LA TABLA CABECERA	
	insert into cabecera( nroresumen, nombre,  apellido, domicilio, nrotarjeta, desde, hasta, vence, total) values(nroresumen, 
	resultadoA.nombre, resultadoA.apellido, resultadoA.domicilio, resultadoB.nrotarjeta, resultadoC.fechainicio, resultadoC.fechacierre,
	resultadoC.fechavto, total);
	
--	INSERTO LOS DATOS EN LA TABLA DETALLE
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
	     insert into detalle( nroresumen, nrolinea,  fecha,  nombrecomercio, monto) values (nroresumen, i,
								resultadoD.fecha, resultadoD.nombre, resultadoD.monto);	     
	end loop;


end

$$ language plpgsql;    `)

	if err != nil {
		log.Fatal(err)
	}

}

func triggeralertadosrechazos() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create or replace function dosrechazosmismodia()  returns trigger as $$

declare

anteultimorechazo record;

begin

select * into anteultimorechazo from rechazo  where  nrotarjeta=new.nrotarjeta and rechazo.motivo= 'supera limite de tarjeta' order by nrorechazo desc limit 1;
--raise notice 'la tarjeta se encuentra suspendida',anteultimorechazo;
if ( anteultimorechazo.fecha = new.fecha and anteultimorechazo.motivo = new.motivo)  then

update tarjeta set  estado = 'suspendida' where tarjeta.nrotarjeta = new.nrotarjeta;

insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) 
values (new.nrotarjeta, current_timestamp, new.nrorechazo, 0, 'Tarjeta suspendida por exceso de limite');
end if;
return new;
 end ; 
 
 $$ language plpgsql;
 
 
create trigger dosrechazosmismodia_trg 
before insert on rechazo
for each row
execute procedure dosrechazosmismodia();`)

	if err != nil {
		log.Fatal(err)
	}

}

func alertanuevorechazo() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create or replace function nuevorechazo()  returns trigger as $$

	
begin
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_date, new.nrorechazo, 0, new.motivo);
	return new;
 end ; 

$$ language plpgsql;

create trigger nuevorechazo_trg 
after insert on rechazo
for each row
execute procedure nuevorechazo() ;`)

	if err != nil {
		log.Fatal(err)
	}

}

func comprasconsecutivasalerta() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create or replace function comprasconsecutivas()  returns trigger as $$

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
	insert into alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion ) values (new.nrotarjeta, current_timestamp, new.nrooperacion, 1, '2 compras en menos de 1 minuto');
	end if;
	return new;
 end ; 

$$ language plpgsql;

create trigger comprasconsecutivas_trg 
before insert on compra
for each row
execute procedure comprasconsecutivas() ;`)

	if err != nil {
		log.Fatal(err)
	}

}

func comprasconsecutivasalertacincominutos() {
	db, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	_, err = db.Exec(`create or replace function comprasmenorcinco()  returns trigger as $$

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
execute procedure comprasmenorcinco() ;`)

	if err != nil {
		log.Fatal(err)
	}

}

func terminarMenu() {
	os.Exit(0)
}

//============================================================
//										NO SQL
//============================================================

func AddJSONData() {
	db1, err := sql.Open("postgres", "user=postgres host=localhost dbname=dbcard sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	tarjetasDB, err := bolt.Open("tarjetas.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer tarjetasDB.Close()

	clienteRow, err := db1.Query(`select * from cliente limit 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer clienteRow.Close()

	var c Cliente

	for clienteRow.Next() {
		err := clienteRow.Scan(&c.Nrocliente, &c.Nombre, &c.Apellido, &c.Domicilio, &c.Telefono)
		if err != nil {
			log.Fatal(err)
		}
		clienteData, err := json.Marshal(c)
		if err != nil {
			log.Fatal(err)
		}
		CreateUpdate(tarjetasDB, "Cliente", []byte(strconv.Itoa(c.Nrocliente)), clienteData)
		resultado1, err := ReadUnique(tarjetasDB, "Cliente", []byte(strconv.Itoa(c.Nrocliente)))
		fmt.Printf(" el resultado > %s\n", resultado1)

	}

	tarjetaRow, err := db1.Query(`select * from tarjeta limit 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer tarjetaRow.Close()
	var t Tarjeta

	for tarjetaRow.Next() {
		if err := tarjetaRow.Scan(&t.Nrotarjeta, &t.Nrocliente, &t.Validadesde, &t.Validahasta, &t.Codseguridad, &t.Limitecompra, &t.Estado); err != nil {
			log.Fatal(err)
		}
		TarjetaData, err := json.Marshal(t)
		if err != nil {
			log.Fatal(err)
		}
		CreateUpdate(tarjetasDB, "Tarjeta", []byte(t.Nrotarjeta), TarjetaData)
		resultado1, err := ReadUnique(tarjetasDB, "Tarjeta", []byte(t.Nrotarjeta))
		fmt.Printf(" el resultado > %s\n", resultado1)
	}

	comercioRow, err := db1.Query(`select * from comercio limit 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer comercioRow.Close()
	var co Comercio

	for comercioRow.Next() {
		if err := comercioRow.Scan(&co.Nrocomercio, &co.Nombre, &co.Domicilio, &co.Codigopostal, &co.Telefono); err != nil {
			log.Fatal(err)
		}
		comercioData, err := json.Marshal(co)
		if err != nil {
			log.Fatal(err)
		}
		CreateUpdate(tarjetasDB, "Comercio", []byte(strconv.Itoa(co.Nrocomercio)), comercioData)
		resultado1, err := ReadUnique(tarjetasDB, "Comercio", []byte(strconv.Itoa(co.Nrocomercio)))
		fmt.Printf(" el resultado > %s\n", resultado1)
	}

	compraRow, err := db1.Query(`select * from compra limit 3`)
	if err != nil {
		log.Fatal(err)
	}
	defer compraRow.Close()
	var com Compra

	for compraRow.Next() {
		if err := compraRow.Scan(&com.Nrooperacion, &com.Nrotarjeta, &com.Nrocomercio, &com.Fecha, &com.Monto, &com.Pagado); err != nil {
			log.Fatal(err)
		}
		compraData, err := json.Marshal(com)
		if err != nil {
			log.Fatal(err)
		}
		CreateUpdate(tarjetasDB, "Compra", []byte(strconv.Itoa(co.Nrocomercio)), compraData)
		resultado1, err := ReadUnique(tarjetasDB, "Compra", []byte(strconv.Itoa(co.Nrocomercio)))
		fmt.Printf(" el resultado > %s\n", resultado1)
	}
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	// abre transacción de escritura
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}

	// cierra transacción
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte

	// abre una transacción de lectura
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})

	return buf, err
}
