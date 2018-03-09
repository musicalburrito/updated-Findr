for file in ./*
do
	while read p; do	
		sed -i "/\b\($p\)\b/d" "$file"
	done < toremove;
done
