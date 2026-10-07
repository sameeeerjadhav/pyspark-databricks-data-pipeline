"""Reference data used to standardize geographic and status values."""

US_STATES = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
    "DC": "District of Columbia",
}

COUNTRY_LOOKUP = {
    "US": "United States",
    "USA": "United States",
    "U.S.": "United States",
    "U.S.A.": "United States",
    "UNITED STATES": "United States",
    "UNITED STATES OF AMERICA": "United States",
    "CA": "Canada",
    "CANADA": "Canada",
    "UK": "United Kingdom",
    "U.K.": "United Kingdom",
    "UNITED KINGDOM": "United Kingdom",
    "GREAT BRITAIN": "United Kingdom",
    "IN": "India",
    "INDIA": "India",
}


def state_lookup() -> dict[str, str]:
    """Map abbreviations and full names to one canonical state name."""
    lookup: dict[str, str] = {}
    for abbreviation, name in US_STATES.items():
        lookup[abbreviation] = name
        lookup[name.upper()] = name
    lookup["ENG"] = "England"
    lookup["ENGLAND"] = "England"
    lookup["ON"] = "Ontario"
    lookup["ONTARIO"] = "Ontario"
    lookup["BC"] = "British Columbia"
    lookup["BRITISH COLUMBIA"] = "British Columbia"
    lookup["MH"] = "Maharashtra"
    lookup["MAHARASHTRA"] = "Maharashtra"
    lookup["KA"] = "Karnataka"
    lookup["KARNATAKA"] = "Karnataka"
    return lookup
