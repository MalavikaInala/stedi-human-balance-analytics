SELECT DISTINCT c.serialnumber, c.customername, c.email, c.phone, c.birthday, c.registrationdate, c.lastupdatedate, c.sharewithpublicasofdate, c.sharewithfriendsasofdate, c.sharewithresearchasofdate 
FROM c INNER JOIN a ON c.email = a.user
