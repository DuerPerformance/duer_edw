{{ config(materialized='table') }}

WITH UNION_TAB as
(
select  "docentry", "linenum", "targettype", "trgetentry", "baseref", "basetype", 
    "baseentry", "baseline", "linestatus", "itemcode", "dscription", 
    "quantity", "shipdate", "openqty", "price", "currency", "rate", 
    "discprcnt", "linetotal", "totalfrgn", "opensum", "opensumfc", 
    "vendornum", "serialnum", "whscode", "slpcode", "commission", 
    "treetype", "acctcode", "taxstatus", "grossbuypr", "pricebefdi", 
    "docdate", "flags", "opencreqty", "usebaseun", "subcatnum", 
    "basecard", "totalsumsy", "opensumsys", "invntsttus", "ocrcode", 
    "project", "codebars", "vatprcnt", "vatgroup", "priceafvat", 
    "height1", "hght1unit", "height2", "hght2unit", "width1", 
    "wdth1unit", "width2", "wdth2unit", "length1", "len1unit", 
    "length2", "len2unit", "volume", "volunit", "weight1", 
    "wght1unit", "weight2", "wght2unit", "factor1", "factor2", 
    "factor3", "factor4", "packqty", "updinvntry", "basedocnum", 
    "baseatcard", "sww", "vatsum", "vatsumfrgn", "vatsumsy", 
    "finncpriod", "objtype", "loginstanc", "blocknum", "importlog", 
    "dedvatsum", "dedvatsumf", "dedvatsums", "isaqcuistn", "distribsum", 
    "dstrbsumfc", "dstrbsumsc", "grssprofit", "grssprofsc", 
    "grssproffc", "visorder", "inmprice", "potrgnum", "potrgentry", 
    "dropship", "polinenum", "address", "taxcode", "taxtype", 
    "origitem", "backordr", "freetxt", "pickstatus", "pickoty", 
    "pickidno", "trnscode", "vatappld", "vatappldfc", "vatappldsc", 
    "baseqty", "baseopnqty", "vatdscntpr", "wtliable", "deferrtax", 
    "equvatper", "equvatsum", "equvatsumf", "equvatsums", "linevat", 
    "linevatlf", "linevats", "unitmsr", "numpermsr", "ceecflag", 
    "tostock", "todiff", "exciseamt", "taxperunit", "totincltax", 
    "countryorg", "stckdstsum", "releasqtty", "linetype", "trantype", 
    "text", "ownercode", "stockprice", "consumefct", "lstbydssum", 
    "stckinmpr", "lstbinmpr", "stckdstfc", "stckdstsc", "lstbydsfc", 
    "lstbydssc", "stocksum", "stocksumfc", "stocksumsc", "stcksumapp", 
    "stckappfc", "stckappsc", "shiptocode", "shiptodesc", "stckappd", 
    "stckappdfc", "stckappdsc", "baseprice", "gtotal", "gtotalfc", 
    "gtotalsc", "distribexp", "descow", "detailsow", "grossbase", 
    "vatwodpm", "vatwodpmfc", "vatwodpmsc", "cfopcode", "cstcode", 
    "usage", "taxonly", "wtcalced", "qtytoship", "delivrdqty", 
    "orderedqty", "cogsocrcod", "ciopplinen", "cogsacct", 
    "chgasmbomw", "actdeldate", "ocrcode2", "ocrcode3", "ocrcode4", 
    "ocrcode5", "taxdistsum", "taxdistsfc", "taxdistssc", "posttax", 
    "excisable", "assblvalue", "rg23apart1", "rg23apart2", 
    "rg23cpart1", "rg23cpart2", "cogsocrco2", "cogsocrco3", 
    "cogsocrco4", "cogsocrco5", "lnexcised", "loccode", "stockvalue", 
    "gpttlbaspr", "unitmsr2", "numpermsr2", "specprice", "cstfipi", 
    "cstfpis", "cstfcofins", "exlineno", "issrvcall", "pqtreqqty", 
    "pqtreqdate", "pcdoctype", "pcquantity", "linmanclsd", 
    "vatgrpsrc", "noinvtrymv", "actbaseent", "actbaseln", "actbasenum", 
    "openrtnqty", "agrno", "agrlnnum", "credorigin", "surpluses", 
    "defbreak", "shortages", "uomentry", "uomentry2", "uomcode", 
    "uomcode2", "fromwhscod", "needqty", "partretire", "retireqty", 
    "retireapc", "retirapcfc", "retirapcsc", "invqty", "openinvqty", 
    "ensetcost", "retcost", "incoterms", "transmod", "linevendor", 
    "distribis", "isdistrb", "isdistrbfc", "isdistrbsc", "isbyprdct", 
    "itemtype", "priceedit", "prntlnnum", "linepoprss", "freechrgbp", 
    "taxrelev", "legaltext", "thirdparty", "lictradnum", "invqtyonly", 
    "unencreasn", "shipfromco", "shipfromde", "fisrtbin", "allocbinc", 
    "exptype", "expuuid", "expoptype", "diotnat", "myftype", 
    "gpbefdisc", "returnrsn", "returnact", "stgseqnum", "stgentry", 
    "stgdesc", "itmtaxtype", "sacentry", "ncmcode", "hsnentry", 
    "oribabsent", "oriblinnum", "oribdoctyp", "isprscgood", 
    "iscstmact", "encryptiv", "exttaxrate", "exttaxsum", "taxamtsrc", 
    "exttaxsumf", "exttaxsums", "stditemid", "commclass", 
    "vatexentry", "vatexln", "natoftrans", "isdtcryimp", "isdtrgnimp", 
    "isorcryexp", "isorrgnexp", "nvecode", "ponum", "poitmnum", 
    "indescala", "cestcode", "ctrsealqty", "cnjpman", "uffiscbene", 
    "cusplit", "legaltimd", "legalttca", "legaltw", "legaltcd", 
    "revcharge", "u_argns_rddate", "u_argns_prepacking", "u_argns_alloc", 
    "u_argns_bal", "u_argns_cnsbo", "u_argns_modcode", 
    "u_argns_modcoded", "u_argns_socut", "u_argns_sostage", 
    "u_argns_socode", "u_argns_pdcno", "u_argns_pdclinenro", 
    "u_argns_sizeruncode", "u_argns_barcode", "u_argns_cntno", 
    "u_allocatedqty", "u_reservedqty", "u_freeqty", "u_cutqty", 
    "u_freeqtytocopy", "u_argns_doctype", "u_argns_docentry", 
    "u_argns_docline", "u_argns_prodgrp", "u_v33_cr", "u_v33__rr", 
    "u_v33_cmr", "u_v33_discountline", "u_qtytoprod", "u_packtype", 
    "u_argns_groupid", "u_argns_position", "u_v33p_externallineid", 
    "u_zeds_comments", "u_zeds_billback", "u_v33_priority", 
    "u_v33_ordrtype", "u_v33s_ic_linenum", "u_argns_advcntno", 
    "u_argns_ordertype", "u_argns_sorefno", "u_argns_sorefdocno", 
    "u_dd_exportcost", "u_argns_trims", "u_argns_servicegrp_id", 
    "u_argns_maxpsize", "u_pmx_loco", "u_pmx_qysc", "u_pmx_ripi", 
    "u_pmx_islc", "u_pmx_iswa", "u_pmx_quan", "u_pmx_batc", 
    "u_pmx_bat2", "u_pmx_bbdt", "u_pmx_luid", "u_pmx_baty", 
    "u_pmx_baen", "u_pmx_bali", "u_pmx_senu", "u_pmx_prdb", 
    "u_pmx_prdr", "u_pmx_prde", "u_pmx_prdl", "u_pmx_sscc", 
    "u_pmx_ex3p", "u_pmx_pqy2", "u_pmx_iqty", "u_pmx_qty2", 
    "u_pmx_uom2", "u_pmx_sulr", "u_pmx_rsnc", "u_pmx_uftx", 
    "u_pmx_spnr", "u_pmx_u2tu", "u_pmx_amba", "u_pmx_sqop", 
    "u_pmx_patq", "u_pmx_paty", "u_pmx_bac1", "u_pmx_bav1", 
    "u_pmx_bac2", "u_pmx_bav2", "u_pmx_bac3", "u_pmx_bav3", 
    "u_pmx_mqu2", "u_pmx_free", "u_pmx_shlf", "u_pmx_sloc", 
    "u_pmx_sqsc", "u_pmx_slui", "u_pmx_mlui", "u_pmx_rlco", 
    "u_all_originalqty", "u_all_linestatus", "u_dd_rei_sku", 
    "u_dd_rei_description" ,'SAP' as Source,'CA' as Source_Region, cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('FIVETRAN_DB', 'CA_DLN1') }}
UNION
select  "docentry", "linenum", "targettype", "trgetentry", "baseref", "basetype", 
    "baseentry", "baseline", "linestatus", "itemcode", "dscription", 
    "quantity", "shipdate", "openqty", "price", "currency", "rate", 
    "discprcnt", "linetotal", "totalfrgn", "opensum", "opensumfc", 
    "vendornum", "serialnum", "whscode", "slpcode", "commission", 
    "treetype", "acctcode", "taxstatus", "grossbuypr", "pricebefdi", 
    "docdate", "flags", "opencreqty", "usebaseun", "subcatnum", 
    "basecard", "totalsumsy", "opensumsys", "invntsttus", "ocrcode", 
    "project", "codebars", "vatprcnt", "vatgroup", "priceafvat", 
    "height1", "hght1unit", "height2", "hght2unit", "width1", 
    "wdth1unit", "width2", "wdth2unit", "length1", "len1unit", 
    "length2", "len2unit", "volume", "volunit", "weight1", 
    "wght1unit", "weight2", "wght2unit", "factor1", "factor2", 
    "factor3", "factor4", "packqty", "updinvntry", "basedocnum", 
    "baseatcard", "sww", "vatsum", "vatsumfrgn", "vatsumsy", 
    "finncpriod", "objtype", "loginstanc", "blocknum", "importlog", 
    "dedvatsum", "dedvatsumf", "dedvatsums", "isaqcuistn", "distribsum", 
    "dstrbsumfc", "dstrbsumsc", "grssprofit", "grssprofsc", 
    "grssproffc", "visorder", "inmprice", "potrgnum", "potrgentry", 
    "dropship", "polinenum", "address", "taxcode", "taxtype", 
    "origitem", "backordr", "freetxt", "pickstatus", "pickoty", 
    "pickidno", "trnscode", "vatappld", "vatappldfc", "vatappldsc", 
    "baseqty", "baseopnqty", "vatdscntpr", "wtliable", "deferrtax", 
    "equvatper", "equvatsum", "equvatsumf", "equvatsums", "linevat", 
    "linevatlf", "linevats", "unitmsr", "numpermsr", "ceecflag", 
    "tostock", "todiff", "exciseamt", "taxperunit", "totincltax", 
    "countryorg", "stckdstsum", "releasqtty", "linetype", "trantype", 
    "text", "ownercode", "stockprice", "consumefct", "lstbydssum", 
    "stckinmpr", "lstbinmpr", "stckdstfc", "stckdstsc", "lstbydsfc", 
    "lstbydssc", "stocksum", "stocksumfc", "stocksumsc", "stcksumapp", 
    "stckappfc", "stckappsc", "shiptocode", "shiptodesc", "stckappd", 
    "stckappdfc", "stckappdsc", "baseprice", "gtotal", "gtotalfc", 
    "gtotalsc", "distribexp", "descow", "detailsow", "grossbase", 
    "vatwodpm", "vatwodpmfc", "vatwodpmsc", "cfopcode", "cstcode", 
    "usage", "taxonly", "wtcalced", "qtytoship", "delivrdqty", 
    "orderedqty", "cogsocrcod", "ciopplinen", "cogsacct", 
    "chgasmbomw", "actdeldate", "ocrcode2", "ocrcode3", "ocrcode4", 
    "ocrcode5", "taxdistsum", "taxdistsfc", "taxdistssc", "posttax", 
    "excisable", "assblvalue", "rg23apart1", "rg23apart2", 
    "rg23cpart1", "rg23cpart2", "cogsocrco2", "cogsocrco3", 
    "cogsocrco4", "cogsocrco5", "lnexcised", "loccode", "stockvalue", 
    "gpttlbaspr", "unitmsr2", "numpermsr2", "specprice", "cstfipi", 
    "cstfpis", "cstfcofins", "exlineno", "issrvcall", "pqtreqqty", 
    "pqtreqdate", "pcdoctype", "pcquantity", "linmanclsd", 
    "vatgrpsrc", "noinvtrymv", "actbaseent", "actbaseln", "actbasenum", 
    "openrtnqty", "agrno", "agrlnnum", "credorigin", "surpluses", 
    "defbreak", "shortages", "uomentry", "uomentry2", "uomcode", 
    "uomcode2", "fromwhscod", "needqty", "partretire", "retireqty", 
    "retireapc", "retirapcfc", "retirapcsc", "invqty", "openinvqty", 
    "ensetcost", "retcost", "incoterms", "transmod", "linevendor", 
    "distribis", "isdistrb", "isdistrbfc", "isdistrbsc", "isbyprdct", 
    "itemtype", "priceedit", "prntlnnum", "linepoprss", "freechrgbp", 
    "taxrelev", "legaltext", "thirdparty", "lictradnum", "invqtyonly", 
    "unencreasn", "shipfromco", "shipfromde", "fisrtbin", "allocbinc", 
    "exptype", "expuuid", "expoptype", "diotnat", "myftype", 
    "gpbefdisc", "returnrsn", "returnact", "stgseqnum", "stgentry", 
    "stgdesc", "itmtaxtype", "sacentry", "ncmcode", "hsnentry", 
    "oribabsent", "oriblinnum", "oribdoctyp", "isprscgood", 
    "iscstmact", "encryptiv", "exttaxrate", "exttaxsum", "taxamtsrc", 
    "exttaxsumf", "exttaxsums", "stditemid", "commclass", 
    "vatexentry", "vatexln", "natoftrans", "isdtcryimp", "isdtrgnimp", 
    "isorcryexp", "isorrgnexp", "nvecode", "ponum", "poitmnum", 
    "indescala", "cestcode", "ctrsealqty", "cnjpman", "uffiscbene", 
    "cusplit", "legaltimd", "legalttca", "legaltw", "legaltcd", 
    "revcharge",
    NULL AS "u_argns_rddate",
    NULL AS "u_argns_prepacking",
    NULL AS "u_argns_alloc",
    NULL AS "u_argns_bal",
    NULL AS "u_argns_cnsbo",
    NULL AS "u_argns_modcode",
    NULL AS "u_argns_modcoded",
    NULL AS "u_argns_socut",
    NULL AS "u_argns_sostage",
    NULL AS "u_argns_socode",
    NULL AS "u_argns_pdcno",
    NULL AS "u_argns_pdclinenro",
    NULL AS "u_argns_sizeruncode",
    NULL AS "u_argns_barcode",
    NULL AS "u_argns_cntno",
    NULL AS "u_allocatedqty",
    NULL AS "u_reservedqty",
    NULL AS "u_freeqty",
    NULL AS "u_cutqty",
    NULL AS "u_freeqtytocopy",
    NULL AS "u_argns_doctype",
    NULL AS "u_argns_docentry",
    NULL AS "u_argns_docline",
    NULL AS "u_argns_prodgrp",
    NULL AS "u_v33_cr",
    NULL AS "u_v33__rr",
    NULL AS "u_v33_cmr",
    NULL AS "u_v33_discountline",
    "u_qtytoprod", "u_packtype", "u_argns_groupid", "u_argns_position", 
    "u_v33p_externallineid", "u_zeds_comments", "u_zeds_billback", 
    "u_v33_priority", "u_v33_ordrtype", "u_v33s_ic_linenum", 
    "u_argns_advcntno", "u_argns_ordertype", "u_argns_sorefno", 
    "u_argns_sorefdocno", "u_dd_exportcost", "u_argns_trims", 
    "u_argns_servicegrp_id", "u_argns_maxpsize", "u_pmx_loco", 
    "u_pmx_qysc", "u_pmx_ripi", "u_pmx_islc", "u_pmx_iswa", 
    "u_pmx_quan", "u_pmx_batc", "u_pmx_bat2", "u_pmx_bbdt", 
    "u_pmx_luid", "u_pmx_baty", "u_pmx_baen", "u_pmx_bali", 
    "u_pmx_senu", "u_pmx_prdb", "u_pmx_prdr", "u_pmx_prde", 
    "u_pmx_prdl", "u_pmx_sscc", "u_pmx_ex3p", "u_pmx_pqy2", 
    "u_pmx_iqty", "u_pmx_qty2", "u_pmx_uom2", "u_pmx_sulr", 
    "u_pmx_rsnc", "u_pmx_uftx", "u_pmx_spnr", "u_pmx_u2tu", 
    "u_pmx_amba", "u_pmx_sqop", "u_pmx_patq", "u_pmx_paty", 
    "u_pmx_bac1", "u_pmx_bav1", "u_pmx_bac2", "u_pmx_bav2", 
    "u_pmx_bac3", "u_pmx_bav3", "u_pmx_mqu2", "u_pmx_free", 
    "u_pmx_shlf", "u_pmx_sloc", "u_pmx_sqsc", "u_pmx_slui", 
    "u_pmx_mlui", "u_pmx_rlco", "u_all_originalqty", 
    "u_all_linestatus", "u_dd_rei_sku", "u_dd_rei_description" ,'SAP' as Source,'US' as Source_Region,cast(current_timestamp() as TIMESTAMP_NTZ) as Insert_Date, NULL AS UpdateDate, 'IO' AS DML 
from  {{ source('FIVETRAN_DB', 'US_DLN1') }}
)

SELECT * FROM UNION_TAB