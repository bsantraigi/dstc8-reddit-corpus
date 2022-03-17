PREFIX="Models_n_Data [Dont_Delete]/2021-SMI-Checkpoints/reddit-dstc8-xtreme/"
L=$(rclone ls onedrive-iitkgp:"Models_n_Data [Dont_Delete]/2021-SMI-Checkpoints/reddit-dstc8-xtreme/" | grep -o "dstc8-.*")
if [ -f links.txt ]; then
	rm links.txt
fi
for line in $L; do
	echo "$line" | tee -a links.txt
	rclone link onedrive-iitkgp:"$PREFIX""$line" | tee -a links.txt
done

