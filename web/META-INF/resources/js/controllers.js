/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you 
 * may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

'use strict';

/* Controllers */
var mudrodControllers = angular.module('mudrodControllers', []);

mudrodControllers.controller('searchCtrl', ['$scope', '$rootScope', '$location', 'Autocomplete', 'SearchOptions',
    function ($scope, $rootScope, $location, Autocomplete, SearchOptions) {

        $scope.hidethis = true;
        $scope.options = {
            opt: 'Or'
        };

        $scope.complete = function (string) {
            Autocomplete.get({term: string},
                function success(response) {
                    $scope.filterSearch = response;
                    $scope.hidethis = false;
                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );
        };

        $scope.fillTextbox = function (string) {
            $scope.options.query = string;
            $scope.hidethis = true;
            document.getElementById("search-btn").focus();
        };

        $scope.search = function (options) {
            $scope.hidethis = true;
            $rootScope.searchOptions = angular.copy(options);
            $location.path("/metadataView/" + options.query + '/' + options.opt);
        };

        $scope.$watch(function () {
            return SearchOptions.getSearchOptions();
        }, function (newValue, oldValue) {
            if (newValue !== null) {
                $scope.options.query = newValue.query;
                $scope.options.opt = newValue.opt;
            }
        }, true);
    }]);

mudrodControllers.controller('vocabularyCtrl', ['$scope', '$rootScope', 'VocabList',
    function vocabularyCtrl($scope, $rootScope, VocabList) {

        var word = $rootScope.searchOptions.query;
        VocabList.get({query: word},
            function success(response) {
                $scope.ontologyList = response.graph.ontology;

            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);

mudrodControllers.controller('metadataViewCtrl', ['$rootScope', '$scope', '$routeParams', 'MetaData', 'PagerService', 'SearchOptions',
    function metadataViewCtrl($rootScope, $scope, $routeParams, MetaData, PagerService, SearchOptions) {
        var vm = this;
        vm.PDItems = [];
        vm.pager = {};
        vm.setPage = setPage;
        vm.rankData = rankData;
        vm.totalMatches = 0;
        vm.rankopt = 'Rank-SVM';


        var word = String();
        var opt = String();
        var rankopt = 'Rank-SVM';

        if (!$routeParams.query) {
            word = $rootScope.searchOptions.query;
            opt = $rootScope.searchOptions.opt;
            SearchOptions.setSearchOptions({'query': word, 'opt': opt});
        } else {
            word = $routeParams.query;
            opt = $routeParams.opt;

            var searchKeyWords = String();
            var topics;

            if (word.search(',') !== -1) {
                topics = word.split(',');
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } else {
                searchKeyWords = word;
            }

            if (searchKeyWords.search('/') !== -1) {
                topics = searchKeyWords.split('/');
                searchKeyWords = "";
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            }

            word = searchKeyWords;

            if (!$rootScope.searchOptions) {
                $rootScope.searchOptions = {};
                opt = 'Or';
            }
            else {
                opt = $rootScope.searchOptions.opt;
            }

            SearchOptions.setSearchOptions({'query': word, 'opt': opt});
            $rootScope.searchOptions.query = searchKeyWords;
            $rootScope.searchOptions.opt = opt;
            searchMetadata();
        }

        function initController() {
            vm.setPage(1);
        }

        function setPage(page) {
            if (page < 1 || page > vm.pager.totalPages) {
                return;
            }

            // get pager object from service
            vm.pager = PagerService.GetPager(vm.PDItems.length, page);

            // get current page of items
            vm.items = vm.PDItems.slice(vm.pager.startIndex, vm.pager.endIndex + 1);
        }

        function rankData(opt) {
            rankopt = opt;
            searchMetadata();
        }

        this.searchTopic = function (topic) {
            var searchKeyWords = String();
            var topics;

            if (topic.search(',') !== -1) {
                topics = topic.split(',');
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            } else {
                searchKeyWords = topic;
            }

            if (searchKeyWords.search('/') !== -1) {
                topics = searchKeyWords.split('/');
                searchKeyWords = "";
                for (var i = 0; i < topics.length; i++) {
                    searchKeyWords += topics[i];
                }
            }

            $rootScope.searchOptions.query = searchKeyWords;
            MetaData.get({query: searchKeyWords, operator: opt},
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    vm.query = word;
                    vm.opt = opt;
                    initController();
                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );
        };

        function searchMetadata() {
            MetaData.get({query: word, operator: opt, rankoption: rankopt},
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    vm.query = word;
                    vm.opt = opt;
                    initController();
                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );
        }
    }]);

mudrodControllers.controller('datasetViewCtrl', ['$rootScope', '$scope', '$routeParams', 'DatasetDetail', 'SearchOptions',
    function datasetViewCtrl($rootScope, $scope, $routeParams, DatasetDetail, SearchOptions) {
        var shortname = $routeParams.shortname;

        var query = String();
        var opt = String();
        if ($rootScope.searchOptions) {
            query = $rootScope.searchOptions.query;
            opt = $rootScope.searchOptions.opt;
            SearchOptions.setSearchOptions({'query': query, 'opt': opt});
        } else if ($routeParams.query && $routeParams.opt) {
            query = $routeParams.query;
            opt = $routeParams.opt;
            SearchOptions.setSearchOptions({'query': query, 'opt': opt});
            $rootScope.searchOptions= SearchOptions
        }

        DatasetDetail.get({shortname: shortname},
            function success(response) {
                var dataAccessUrls;

                $scope.dataset = response.PDResults[0];
                var dataAccessUrl = $scope.dataset['DatasetLocationPolicy-BasePath'];
                if (dataAccessUrl.search(',') !== -1) {
                    dataAccessUrls = dataAccessUrl.split(',');
                    $scope.ftpUrl = dataAccessUrls[0];
                    $scope.httpsUrl = dataAccessUrls[1];
                    $scope.hideUrls = false;
                    $scope.hideUrl = true;
                } else {
                    $scope.hideUrl = false;
                    $scope.hideUrls = true;
                }
            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);

mudrodControllers.controller('hRecommendationCtrl', ['$scope', '$routeParams', 'HRecommendation',
    function hRecommendationCtrl($scope, $routeParams, HRecommendation) {
        var shortname = $routeParams.shortname;
        HRecommendation.get({shortname: shortname},
            function success(response) {
                $scope.recommendationList = response.HybridRecommendationData.linked;

            },
            function error(errorResponse) {
                console.log("Error:" + JSON.stringify(errorResponse));
            }
        );
    }]);



