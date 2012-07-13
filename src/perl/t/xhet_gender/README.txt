Tests for mixture model to compute xhet gender thresholds.

Test data sampled from a mixture model with added noise, using concoctXhet.py with 100 samples and other params as follows, where N denotes a third 'noise' component:

lambdaN = 0.05
lambdaM = 0.5 - amb/2  
lambdaF = lambdaM
muM = 0.01
muF = 0.25
muN = abs(muF-muM)/2
sigM = 0.002 
sigF = 0.03
sigN = abs(sigF-sigM)/2

Update 2012-07-13:  The data and BASH script are still useful for a standalone test of the gender check, but preferable to use src/perl/t/qc.t which does a test of the entire QC setup using the standard Perl testing framework.
