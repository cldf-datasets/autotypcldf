"""
select
    l.cldf_name,
    json_extract(value, '$.IndividualPredicateID'),
    json_extract(value, '$.IndividualPredicateMeaning')
from
    languagetable as l,
    valuetable as v,
    json_tree(v.cldf_value, '$.IndividualPredicates')
where
    v.cldf_languageReference = l.cldf_id and
    v.cldf_parameterReference = '75' and
    type = 'object';


select
    l.cldf_name,
    count(v.cldf_id) as c,
    group_concat(json_extract(v.cldf_value, '$.PredicateClassLabel'), '; ')
from
    valuetable as v,
    parametertable as p,
    languagetable as l
where
    v.cldf_languageReference = l.cldf_id and
    v.cldf_parameterReference = p.cldf_id and
    p.cldf_name = 'PredicateClasses'
group by
    v.cldf_languageReference
order by c desc limit 5;
"""
def run(args):
    """
    select p.module, count(v.cldf_id) from parametertable as p, valuetable as v where v.cldf_parameterReference = p.cldf_id group by p.module;
Categories|5782
GrammaticalRelations|179313
Morphology|56088
NP|4364
Sentence|6767
VerbInflection|25339
Word|881


select p.module, count(distinct v.cldf_languageReference) from parametertable as p, valuetable as v where v.cldf_parameterReference = p.cldf_id group by p.module;
Categories|505
GrammaticalRelations|811
Morphology|999
NP|485
Sentence|468
VerbInflection|229
Word|76

select p.module, count(p.cldf_id) from parametertable as p where p.kind = 'manual data entry' group by p.module;
Categories|14
GrammaticalRelations|5
Morphology|5
NP|1
Sentence|27
VerbInflection|66
Word|1

    """
    # check whether we have to create the database
    pass