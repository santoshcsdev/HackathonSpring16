

angular.module("app",[])
    .controller("Ctrl", function($scope,$http){

        var Idconnection, ProductId, i, j, NameConnection, Rating, x = 0;
        $scope.OverallRating = new Array();
        $scope.DecisionText;
        $scope.ORating;

        $scope.getReview = function(productname){
            Idconnection = $http.get("http://0.0.0.0:8586/products?query="+productname);
            //console.log(productname);
            Idconnection.success(function(data){
                //console.log(i);
                if(data!=null){
                    for(i=0;i<data.hits.length;i++){

                        ProductId = data.hits[i]._id;
                        console.log(data.hits[i]);
                        NameConnection = $http.get("http://0.0.0.0:8586/reviews?product="+ProductId);
                        NameConnection.success(function(index, response){
                            $scope.ORating = 0;
                            // console.log(index);
                            //console.log("i=" + i);

                            //console.log(response);
                            //console.log(response.hits[0]);
                            for(j=0;j<response.hits.length;j++){
                                if (response.hits[j]._source != null){
                                    Rating = response.hits[j]._source.overall;
                                    console.log(response.hits[j]);
                                    $scope.ORating = $scope.ORating + Rating;
                                }
                                else {
                                    $scope.ORating = $scope.ORating + 0;
                                }
                            }
                            //console.log($scope.ORating);
                            $scope.ORating = $scope.ORating/j;
                            if (data.hits[index]._source.price == null)
                            {
                                data.hits[index]._source.price = "Not Available"
                            }
                            if ($scope.ORating > 0 && $scope.ORating < 2){
                                $scope.DecisionText = "Buying this product may be the worst idea"
                            }
                            else if ($scope.ORating > 2 && $scope.ORating < 3){
                                $scope.DecisionText = "You may regret about your decision of buying this product"
                            }
                            else if ($scope.ORating >= 3 && $scope.ORating < 3.5){
                                $scope.DecisionText = "This product might meet your requirements but may not satisfy to higher extents"
                            }
                            else if ($scope.ORating >=3.5 && $scope.ORating < 4){
                                $scope.DecisionText = "You will completely satisfy with the product"
                            }
                            else if ($scope.ORating >=4 && $scope.ORating < 4.5){
                                $scope.DecisionText = "A very good product with high reliability quotient and can trust this product anytime, anywhere"
                            }
                            else if ($scope.ORating >= 4.5 && $scope.ORating <= 5){
                                $scope.DecisionText = "An outstanding product and idea of buying it might be one of the best decision of your all judgements"
                            }
                            $scope.OverallRating[index] = {
                                "Rating": $scope.ORating,
                                "Decision": $scope.DecisionText,
                                "Name": data.hits[index]._source.title,
                                "Price": data.hits[index]._source.price,
                                "Image": data.hits[index]._source.imUrl
                            };
                        }.bind(this, i))
                        // console.log($scope.ORating);
                        // console.log(data.hits[i]);
                        // console.log(i);

                    }
                }
            })
        }
    });
