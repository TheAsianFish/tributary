{# Project family of a Wikimedia domain: wikipedia, wiktionary, wikidata, commons, ... #}
{% macro project_family(domain) -%}
    case
        -- Shared single-sign-on domain: account creations of every wiki are logged here.
        when {{ domain }} = 'auth.wikimedia.org' then null
        when {{ domain }} = 'commons.wikimedia.org' then 'commons'
        when {{ domain }} = 'species.wikimedia.org' then 'wikispecies'
        when {{ domain }} like '%.wikimedia.org' then 'wikimedia'
        when {{ domain }} like '%.wikidata.org' then 'wikidata'
        when {{ domain }} = 'www.wikifunctions.org' then 'wikifunctions'
        when {{ domain }} = 'www.mediawiki.org' then 'mediawiki'
        when {{ domain }} = 'abstract.wikipedia.org' then 'abstract_wikipedia'
        else substring({{ domain }} from '\.([a-z]+)\.org$')
    end
{%- endmacro %}

{#
  Language subdomain of a per-language project (en.wikipedia.org -> en). NULL for
  multilingual projects (commons, wikidata, meta, ...) and for test wikis.
#}
{% macro language_code(domain) -%}
    case
        when {{ domain }} !~ '\.(wikipedia|wiktionary|wikiquote|wikisource|wikibooks|wikinews|wikiversity|wikivoyage)\.org$'
            then null
        when split_part({{ domain }}, '.', 1) in ('www', 'test', 'test2', 'abstract') then null
        when split_part({{ domain }}, '.', 1) = 'simple' then 'en'
        else split_part({{ domain }}, '.', 1)
    end
{%- endmacro %}
