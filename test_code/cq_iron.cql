match (h:Hero)
where h.name starts with "IRON"
return h
order by h.degree desc;
