Python classes for rapid testing of xhet gender model.  Scripts are as follows:

concoctXhet.py 	   Create fake xhet data, sampled from a 3-component mixture distribution.  Components represent male, female, and noise.  Write in standard sample_xhet_gender.txt format.

stabilityTest.py   Generate a fake xhet dataset with given parameters, and repeatedly train the mixture model to test stability.  (Default normalmixEM training in R mixtools package is not fully deterministic, as starting param values are random.)

testRange.py	   As for stability test, but repeated for multiple sizes of dataset, and generating multiple datasets at each size.
