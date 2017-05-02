'use strict';

/* Controllers */
var mudrodControllers = angular.module('mudrodControllers', []);

mudrodControllers.controller('searchCtrl', ['$scope', '$rootScope', '$location', 'Autocomplete',
    function ($scope, $rootScope, $location, Autocomplete) {
        $scope.options = {
            preference: 'Phrase'
        };
        $scope.relatedTerms = [];
        $rootScope.searchOptions = {};
        
        $scope.complete = function(string) {  
            $scope.hidethis = false;  
            var output = [];  
            Autocomplete.get({term: string},
                function success(response) {
                    //alert($scope.challenge.question);
                    //console.log("Success:" + JSON.stringify(response));
                    //$scope.blogList = response;
                    $scope.filterSearch = response;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
            );  
        }  

        $scope.fillTextbox = function(string){  
            $scope.options.term = string;  
            $scope.hidethis = true;  
        }  

        $scope.search = function(options) {
            $rootScope.searchOptions = angular.copy(options);
            
            $location.path("/metadataView/"+options.term);

        };

    }]);
    
mudrodControllers.controller('vocabularyCtrl', ['$scope', '$rootScope', 'VocabList',
    function vocabularyCtrl($scope, $rootScope, VocabList) {

        var word = $rootScope.searchOptions.term;
        var opt = $rootScope.searchOptions.preference;
        VocabList.get({query: word},
                function success(response) {
                    //alert($scope.challenge.question);
                    //console.log("Success:" + JSON.stringify(response));
                    //$scope.blogList = response;
                    $scope.ontologyList = response.graph.ontology;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('metadataViewCtrl', ['$rootScope', '$routeParams', 'MetaData', 'PagerService',
    function metadataViewCtrl($rootScope, $routeParams, MetaData, PagerService) {
        var vm = this;
        vm.PDItems = [];
        vm.pager = {};
        vm.setPage = setPage;
        vm.totalMatches = 0;

        var word;
        var opt;
        
        if(!$routeParams.name){
            word = $rootScope.searchOptions.term;    
            opt = $rootScope.searchOptions.preference; 
        } else {
            word = $routeParams.name;
            $rootScope.searchOptions.term = word;
            opt = 'And';
        }

        function initController() {
            // initialize to page 1
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

        MetaData.get({query: word, operator: opt}, 
                function success(response) {
                    vm.PDItems = response.PDResults;
                    vm.totalMatches = vm.PDItems.length;
                    initController();
                    //$scope.PDResults = response.PDResults;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('datasetViewCtrl', ['$scope', '$routeParams', 'DatasetDetail',
    function metadataViewCtrl($scope, $routeParams, DatasetDetail) {
        var shortname = $routeParams.shortname;   
        DatasetDetail.get({shortname: shortname}, 
                function success(response) {
                    //alert($scope.challenge.question);
                    // console.log("Success:" + JSON.stringify(response));
                    $scope.dataset = response.PDResults[0];

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);

mudrodControllers.controller('hRecommendationCtrl', ['$scope', '$routeParams', 'HRecommendation',
    function metadataViewCtrl($scope, $routeParams, HRecommendation) {
        var shortname = $routeParams.shortname;   
        HRecommendation.get({shortname: shortname}, 
                function success(response) {
                    //alert($scope.challenge.question);
                    // console.log("Success:" + JSON.stringify(response));
                    $scope.recommendationList = response.HybridRecommendationData.linked;

                },
                function error(errorResponse) {
                    console.log("Error:" + JSON.stringify(errorResponse));
                }
        );
    }]);