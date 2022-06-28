SELECT	FORMAT(docE.DO_Date,'yyyy-MM')
     ,docE.DataBaseSource
     ,cai.DE_No
     ,cai.DE_NoSource
     ,cai.DE_Intitule
     ,SUM(docL.DL_MontantHT)
FROM F_DOCLIGNE docL
         INNER JOIN F_DOCENTETE docE
                    ON docE.Do_Domaine = docL.DO_Domaine
                        AND docE.DO_Type = docL.DO_Type
                        AND docE.DO_Piece = docL.DO_Piece
         LEFT JOIN F_DEPOT cai ON cai.DE_No = docE.DE_No
WHERE FORMAT(docE.DO_Date,'yyyy-MM') = '2022-01'
--AND FORMAT(docE.DO_Date,'yyyy-MM-dd') <= '2022-05-20'
--AND 
--CA_Intitule  = '02HAYATOU'
GROUP BY FORMAT(docE.DO_Date,'yyyy-MM')
       ,docE.DataBaseSource
       ,cai.DE_No
       ,cai.DE_NoSource
       ,cai.DE_Intitule
ORDER BY 1,2

SELECT	FORMAT(docE.DO_Date,'yyyy-MM')
     ,docE.DataBaseSource
     ,cai.CA_No
     ,cai.CA_NoSource
     ,cai.CA_Intitule
     ,SUM(docL.DL_MontantHT)
FROM F_DOCLIGNE docL
         INNER JOIN F_DOCENTETE docE
                    ON docE.Do_Domaine = docL.DO_Domaine
                        AND docE.DO_Type = docL.DO_Type
                        AND docE.DO_Piece = docL.DO_Piece
         LEFT JOIN F_CAISSE cai ON cai.CA_No = docE.CA_No
WHERE FORMAT(docE.DO_Date,'yyyy-MM') = '2022-01'
--AND FORMAT(docE.DO_Date,'yyyy-MM-dd') <= '2022-05-20'
--AND
--CA_Intitule  = '02HAYATOU'
GROUP BY FORMAT(docE.DO_Date,'yyyy-MM')
       ,docE.DataBaseSource
       ,cai.CA_No
       ,cai.CA_NoSource
       ,cai.CA_Intitule
ORDER BY 1,2