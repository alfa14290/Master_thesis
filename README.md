# Master_thesis
Bond Returns Prediction

# Step1: Bond filteration:
# Filter only corporate bonds with majority of trading volume:

CDEB BOND_TYPE US Corporate Debentures --yes (long term)
CMTN BOND_TYPE US Corporate MTN --yes (medium term)
CMTZ BOND_TYPE US Corporate MTN Zero ----if analysed have to be analyzed seperately as no interest
CPIK BOND_TYPE US Corporate PIK Bond --form of mezzanine debt that lessens the financial burden of making cash coupon payments to investors.(https://www.investopedia.com/terms/p/pikbond.asp)
USBN BOND_TYPE US Corporate Bank Note
CZ BOND_TYPE US Corporate Zero ----if analysed have to be analyzed seperately as no interest



    CCPI BOND_TYPE US Corporate Inflation Indexed ----later because of inflation to check with the prediction
    CLOC BOND_TYPE US Corporate LOC Backed -----------Not because loc or asset backed
    CS BOND_TYPE US Corporate Strip-----   only 63 and principal and coupon are removed (may be later)(https://www.investopedia.com/terms/s/stripbond.asp)
    CCOV BOND_TYPE US Corporate Convertible --because of convertible 
    CP BOND_TYPE US Corporate Paper --short term and unsecured(create biase ) and finance short liabilities such as pyrool, accounts
    CPAS BOND_TYPE US Corporate Pass Thru Trust --https://www.investopedia.com/terms/p/passthroughsecurity.asp
    CUIT BOND_TYPE US Corporate UIT ---portfolio of bonds offered by like of mutual funds or trust companies
    UCID BOND_TYPE US Corporate Insured Debenture---not very important and held until maturity anyways

