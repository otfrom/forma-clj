(ns forma.trends.data
  (use [forma.trends.stretch])
  (require [forma.schema :as schema]))

;; Data come from tile 2809 chunk 3, pixel 0, 0.
(def ndvi
  [6217 8599 7074 8437 8471 8285 8342 9035 8356 7612 8538 6439 8232 7277 8588 7651 8824 4981 7251 7332 6179 4618 6506 8188 8320 6262 8094 8129 6773 7230 6417 6791 6285 6013 5786 8020 7588 6423 5734 6522 6481 7924 8067 7328 4249 8490 8591 4472 6335 8706 8076 8376 8861 8183 8712 6426 8314 8441 6643 7673 5193 8813 7902 7275 4480 7004 5691 5630 7540 8610 8981 5181 8947 8681 9072 8931 8879 8770 8702 6578 9027 8846 8530 6927 9128 5984 6133 4775 6707 6707 3392 7081 5806 6580 9108 5748 6784 8520 8597 9130 7585 6531 6768 7249 4992 4048 7988 8088 7418 4082 8056 2715 1899 8579 8852 8896 3010 8063 7985 8377 5503 8139 8672 8319 5995 8252 8835 8593 8909 6817 8488 7206 8561 8549 4261 5659 5924 8601 7302 2610 7610 7416 8978 8704 8528 8236 5400 6372 8387 9279 9175 8652 4637 4167 5624 5707 5404 4369 8607 2557 7840 9053 9502 8350 5512 8692 8274 4387 8192 8341 8042 6401 6284 7568 8354 7423 7064 4733 8441 5717 6456 4626 8160 7142 7135 5727 6847 8186 8179 8377 6998 6936 6722 5768 8552 6355 7360 7270 6069 3242 4972 5147 3720 6407 3887 6666 4915 5367 7383 5451 7442 7432 7807 7115 8622 7946 8488 7488 5482 4718 8206 8280 8822 8530 7810 8141 3207 5628 7737 7662 8606 8226 6252 8267 8668 8808 4407 8330 7473 8432 9038 7924 8581 9452 8347 5745 8741 5246 7643 6559 7669 3969 7377 4248 3973 8415 8031 8867 8481 4967 8804 8598 7935 8080 5842 8896 8789 7931 5019 8511 5378])

(def qual [2062 36074 35878 2120 34965 2116 34961 34837 2116 2261 3226 3098 2257 34841 34841 2062 2061 2066 3102 35878 3098 2066 34837 2185 34893 2066 2061 2116 34837 35998 2185 2261 2116 2116 2116 2257 2261 2116 2062 36074 35874 2120 34837 34837 34838 2257 34837 2062 3102 35037 34837 34961 34965 2185 34961 34897 2116 2116 2062 35037 3098 2257 34837 35874 2066 2185 34841 2066 34837 2116 2261 2066 34837 34966 2120 2189 2185 2120 2189 35037 2261 2116 2116 3098 2185 2062 34842 34842 3098 3098 2062 34837 2062 34838 34961 34841 2062 2185 35037 34837 34961 34841 35037 34837 34837 34837 2120 2257 2062 2066 34837 2066 2066 2257 2261 2061 34837 2116 34837 2262 2066 34837 2116 2261 2062 2116 2185 2189 2189 2066 36074 35878 34837 2116 2066 34837 34837 34837 35033 2066 34842 3098 2120 34837 2120 34893 34841 2062 2116 2261 34841 2257 34842 2062 2062 34841 3098 2062 2189 2062 34837 34965 35037 3226 34841 2185 2116 2185 34893 2261 35874 2062 34837 36002 2185 2185 35874 2066 34837 2062 2062 2066 2261 35874 34837 2062 35878 2116 2185 2261 2066 2116 2185 34837 2261 34961 2116 34841 2062 2066 34893 2120 2062 2066 2066 35878 2066 2062 2262 35874 2116 34965 35874 34837 2185 2116 2261 2116 35878 34837 2061 2257 2261 2189 2116 34842 2062 3098 34837 2062 2185 34837 34837 34961 2185 34837 34837 2116 34837 2116 2116 34893 2116 2189 3226 34841 2120 2062 35878 2062 35874 2062 34961 34841 2062 3298 35878 2189 2116 34965 2185 2189 2116 2185 34961 35033 2120 2185 34837 2120 2062])

(def reli [3 3 3 0 1 0 1 1 0 1 3 3 1 1 1 3 1 3 3 3 3 3 1 1 1 3 1 0 1 3 1 1 0 0 0 1 1 0 3 3 3 1 1 1 3 1 1 3 3 1 1 1 1 1 1 1 0 0 3 1 3 1 1 3 3 1 1 3 1 0 1 3 1 3 1 1 1 1 1 1 1 0 0 3 1 3 3 3 3 3 3 1 3 3 1 1 3 1 1 1 1 1 1 1 1 1 0 1 3 3 1 3 3 1 1 1 1 0 1 3 3 1 0 1 3 0 1 1 1 3 3 3 1 0 3 1 1 1 1 3 3 3 1 1 0 1 1 3 0 1 1 1 3 3 3 1 3 3 1 3 1 1 1 3 1 1 0 1 1 1 3 3 1 3 1 1 3 3 1 3 3 3 1 3 1 3 3 0 1 1 3 0 1 1 1 1 0 1 3 3 1 1 3 3 3 3 3 3 3 3 0 1 3 1 1 0 1 0 3 1 1 1 1 1 0 3 3 3 1 3 1 1 1 1 1 1 1 0 1 0 0 1 0 1 3 1 1 3 3 3 3 3 1 1 3 3 3 1 0 1 1 1 0 1 1 1 1 1 1 1 3])

(def evi [4807 5211 5299 6513 6548 5965 4203 6340 5049 5773 6321 6476 6089 4730 6268 6286 6712 5339 6416 6140 7370 5761 5752 5811 5531 5264 6257 5912 4091 4329 2663 4005 3073 2889 3129 4177 4953 3309 3728 4635 4710 5721 5208 4736 5275 5068 5654 5828 5929 5598 5175 4828 4474 5184 6889 5941 5659 5713 4543 7799 5144 6185 5574 5435 6134 4612 5372 6180 6621 5955 5071 4122 4943 6254 5081 5062 4952 6119 6107 7422 6114 4580 4827 5746 4683 5644 5690 5694 4752 4752 5221 5559 4154 4602 4511 2980 4565 4580 5975 4950 4565 4982 3917 4566 3848 4859 5078 4789 4223 4810 4751 4152 3895 5326 6034 5774 3956 5047 4350 5482 4732 4775 4520 4557 4475 4501 4284 4485 4468 5134 5771 5049 4898 5265 5023 4831 4731 5774 5259 4403 5824 5205 4615 2788 5495 4628 5634 4599 4619 6949 4921 5437 6446 4860 4768 5867 4261 4982 6092 3703 4561 4355 4982 5947 6000 5289 5124 4445 4520 5349 5107 5447 3739 4681 4474 5229 4154 4314 5054 4728 4639 4969 5774 5017 5026 4984 4809 5121 5556 5618 5605 4125 3685 3155 4469 3565 4029 5458 5160 4380 2237 2968 3780 4937 4653 4786 5079 3988 4501 4633 4100 4046 5893 3125 5647 5436 6142 4789 5153 5268 6201 4916 6484 5936 5641 7187 4962 6004 5681 6061 4545 3532 4543 4456 4720 4439 4125 4128 4706 4524 4774 4191 4474 5008 5869 5477 4545 5047 5321 4426 4340 3350 3431 3677 2986 6265 4773 4403 4361 5466 4726 5682 3987 3993 5512 5465 4100 4566 5006 5365 3305])

(def Yt [0.9 0.89 0.88 0.88 0.89 0.9 0.89 0.89 0.87 0.88 0.86 0.83 0.8 0.77 0.76 0.82 0.79 0.82 0.83 0.82 0.84 0.82 0.86 0.89 0.87 0.9 0.88 0.87 0.89 0.9 0.84 0.87 0.85 0.81 0.76 0.74 0.77 0.79 0.75 0.69 0.69 0.69 0.72 0.72 0.76 0.78 0.78 0.82 0.85 0.85 0.83 0.86 0.85 0.86 0.84 0.82 0.82 0.81 0.78 0.75 0.77 0.73 0.75 0.73 0.75 0.77 0.77 0.76 0.77 0.79 0.79 0.79 0.81 0.85 0.84 0.87 0.86 0.82 0.82 0.83 0.81 0.79 0.79 0.76 0.79 0.75 0.77 0.8 0.8 0.83 0.84 0.85 0.85 0.84 0.84 0.84 0.88 0.86 0.85 0.86 0.86 0.86 0.85 0.84 0.73 0.62 0.66 0.58 0.52 0.45 0.42 0.39 0.4 0.42 0.48 0.52 0.56 0.52 0.49 0.49 0.45 0.47 0.53 0.48 0.44 0.42 0.42 0.42 0.38 0.32 0.34 0.31 0.3 0.3 0.3 0.29 0.34 0.32 0.33 0.35 0.36 0.37 0.38 0.38 0.38 0.45 0.4 0.37 0.39 0.38 0.33 0.31 0.33 0.39 0.47 0.43 0.41 0.43 0.43 0.43 0.45 0.46 0.49 0.52 0.55 0.6 0.59 0.65 0.64 0.64 0.61 0.62 0.55 0.54 0.54 0.56 0.53 0.59 0.63 0.68 0.69 0.68 0.74 0.72 0.71 0.68 0.7 0.69 0.73 0.72 0.72 0.73 0.76 0.71 0.76 0.69 0.65 0.64 0.68])

(def rain-raw [0.0 0.0 0.0 0.2 0.2 2.1 5.3 3.4 2.2 0.6 0.0 0.0 0.0 0.0 0.0 0.0 0.4 2.2 4.3 4.2 2.4 0.2 0.0 0.0 0.0 0.0 0.0 0.3 0.5 2.9 3.4 4.5 1.9 1.5 0.0 0.0 0.0 0.0 0.0 0.2 1.3 2.8 4.3 6.7 2.5 1.0 0.0 0.0 0.4 0.0 0.0 0.4 0.5 1.4 4.0 3.9 2.0 0.2 0.0 0.0 0.0 0.0 0.0 0.1 1.7 3.8 4.3 5.1 2.7 0.4 0.0 0.0 0.0 0.0 0.0 0.0 0.3 1.6 3.7 5.3 1.9 0.5 0.0 0.0 0.0 0.0 0.0 0.3 0.9 1.5 4.7 5.8 2.4 0.1 0.0 0.0 0.0 0.0 0.1 0.1 0.7 3.2 3.8 5.4 1.6 0.4 0.0 0.0 0.0 0.0 0.0 0.0 0.4 2.4 3.3 5.6 2.4 0.8 0.0 0.0 0.0 0.0 0.0 0.2 0.3 2.2 4.3 4.6 2.6 1.1 0.0 0.0 0.0 0.0 0.0 0.0 0.4 2.4 3.3 5.6 2.4 0.8 0.0 0.0 0.0 0.0 0.0 0.2 0.3 2.2 4.3 4.6 2.6 1.1 0.0 0.0])


(def rain-ts
  (schema/timeseries-value 360 rain-raw))

(def rain-ts-expanded
  (ts-expander "32" "16" rain-ts))


(def rain (take (count ndvi) (:series rain-ts-expanded)))
