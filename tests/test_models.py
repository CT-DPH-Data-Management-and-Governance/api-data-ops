from dataops.models import acs


def test_from_url_multi_geo():
    url = "https://api.census.gov/data/2023/acs/acs5?get=group(B19013H)&ucgid=pseudo(0400000US09$0600000)"
    cls = acs.APIEndpoint.from_url(url)

    assert cls.base_url == "https://api.census.gov/data"
    assert cls.dataset == "acs/acs5"
    assert cls.year == 2023
    assert cls.variables == ["group(B19013H)"]
    assert cls.geography == "ucgid:pseudo(0400000US09$0600000)"
    assert cls.variable_url == "https://api.census.gov/data/2023/acs/acs5/variables"
    assert (
        cls.url_no_key
        == "https://api.census.gov/data/2023/acs/acs5?get=group%28B19013H%29&ucgid=pseudo%280400000US09%240600000%29"
    )


def test_from_url_subject():
    url = "https://api.census.gov/data/2021/acs/acs5/subject?get=group(S2701)&ucgid=0400000US09"
    cls = acs.APIEndpoint.from_url(url)

    assert cls.base_url == "https://api.census.gov/data"
    assert cls.dataset == "acs/acs5/subject"
    assert cls.year == 2021
    assert cls.variables == ["group(S2701)"]
    assert cls.geography == "ucgid:0400000US09"
    assert (
        cls.variable_url
        == "https://api.census.gov/data/2021/acs/acs5/subject/variables"
    )
    assert (
        cls.url_no_key
        == "https://api.census.gov/data/2021/acs/acs5/subject?get=group%28S2701%29&ucgid=0400000US09"
    )
