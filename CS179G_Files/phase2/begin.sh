for file in ./*
do
	echo 'series_id	seasonal	areatype_code	industry_code	occupation_code	datatype_code	state_code	area_code	sector_code	series_title	footnote_codes	begin_year	begin_period	end_year	end_period' | cat - "$file" > temp && mv temp "$file"
done
