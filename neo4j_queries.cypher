MATCH p=(album:ALBUM)<--(a:ARTIST)-->(s:SONG)-->(album:ALBUM) 
WHERE a.name = "Madonna" RETURN p

// Madonna Songs + Albums
MATCH p=(a:ARTIST)-->(s:SONG)-->(album:ALBUM)
WHERE a.name = "Madonna"
RETURN p

// Madonna One album
MATCH p=(a:ARTIST)-->(s:SONG)-->(album:ALBUM)
WHERE a.name = "Madonna" AND album.name = "Like A Virgin" 
RETURN p

// List All Albums
MATCH (album:ALBUM)<--(a:ARTIST)-->(s:SONG)-->(album:ALBUM)
WHERE a.name = "Madonna"  
RETURN distinct album.name

// #Songs per Year 
MATCH (n:YEAR)<--(x:SONG)
RETURN n.year, count(*) as count_songs
ORDER BY n.year

// #Songs per Year (sort desc)
MATCH (n:YEAR)<--(x:SONG)
RETURN n.year, count(*) as count_songs
ORDER BY count_songs desc

// #Songs per Artist
MATCH (n:ARTIST)-->(x:SONG)
RETURN n.name, count(*) as count_songs
ORDER BY count_songs desc

// #Songs and #Albums per Artist
MATCH (n:ARTIST)-->(x:SONG)-->(a:ALBUM)
RETURN n.name, count(distinct x) as count_songs, count(distinct a)
ORDER BY count_songs desc

// Same or different song?
MATCH p=(:ARTIST)-->(s:SONG) 
WHERE LOWER(s.title) = "blue christmas" RETURN p
