SELECT ST_AsText(ST_Collect(ST_GeomFromText('POINT(1 2)'),
	ST_GeomFromText('POINT(-2 3)') ));