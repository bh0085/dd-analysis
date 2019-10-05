

-- -- UPDATE segment 
-- -- SET points_xym_total_reads = sq.new_geo
-- -- FROM (
-- --     SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy), umi.total_reads)) as new_geo
-- --     FROM segment JOIN umi ON umi.seg = segment.id
-- --     WHERE segment.dsid=29045011
-- --     GROUP BY segment.id
-- -- ) AS sq
-- -- WHERE segment.id = sq.seg_id;

-- -- UPDATE segment 
-- -- SET points_xym_kde = sq.new_geo
-- -- FROM (
-- --     SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy), umi.kde_val)) as new_geo
-- --     FROM segment JOIN umi ON umi.seg = segment.id
-- --     WHERE segment.dsid=29045011
-- --     GROUP BY segment.id
-- -- ) AS sq
-- -- WHERE segment.id = sq.seg_id;




-- -- UPDATE segment 
-- -- SET points_xym_total_reads_rank = sq.new_geo
-- -- FROM (
-- --     SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy), umi.total_reads)) as new_geo
-- --     FROM segment JOIN umi ON umi.seg = segment.id
-- --     WHERE segment.dsid=29045011
-- --     GROUP BY segment.id
-- -- ) AS sq
-- -- WHERE segment.id = sq.seg_id;

-- -- UPDATE segment 
-- -- SET points_xym_kde_rank = sq.new_geo
-- -- FROM (
-- --     SELECT segment.id AS seg_id, ST_Collect(ST_MakePointM(ST_X(umi.xumi_xy),ST_Y(umi.xumi_xy), umi.kde_val)) as new_geo
-- --     FROM segment JOIN umi ON umi.seg = segment.id
-- --     WHERE segment.dsid=29045011
-- --     GROUP BY segment.id
-- -- ) AS sq
-- -- WHERE segment.id = sq.seg_id;



-- SELECT ST_Collect(pts.pt)
-- FROM(
-- SELECT    g as pt
--      FROM
--     (
--         SELECT ST_DumpPoints(segment.points_xym_kde ) AS gdump
--         FROM segment WHERE  id=295409
--     ) AS g
--     ORDER BY ST_M((g.gdump).geom) DESC
--     LIMIT 100
-- ) AS pts

-- -- SELECT     st_asgeojson((g.gdump).geom,3) as pt
-- --      FROM
-- --     (
-- --         SELECT ST_DumpPoints(segment.points_xym_kde ) AS gdump
-- --         FROM segment WHERE  id=295409
-- --     ) AS g
-- --     ORDER BY ST_M((g.gdump).geom) DESC
-- --     LIMIT 100



-- UPDATE segment SET hull = ch.hull
-- FROM(
-- SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
-- FROM umi JOIN segment ON segment.id= umi.seg
-- WHERE segment.n_umis>20 
-- AND segment.dsid=01409003
-- GROUP BY umi.seg 
-- ) as ch
-- WHERE ch.seg = segment.id
-- AND segment.dsid=01409003;

-- UPDATE umi 
-- SET xumi_xy=ST_SetSRID(ST_MakePoint(x, y),4326) 
-- WHERE umi.dsid=01409003;


SELECT ch.hull 
 FROM segment, (
SELECT umi.seg as seg, ST_ConvexHull(ST_Collect(xumi_xy))  as hull
FROM umi JOIN segment ON segment.id= umi.seg
WHERE segment.n_umis>20 
AND segment.dsid=01409003
GROUP BY umi.seg 
) as ch
WHERE ch.seg = segment.id
AND segment.dsid=01409003;