package brightroll

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/golang/glog"
	"github.com/mxmCherry/openrtb"
	"github.com/prebid/prebid-server/adapters"
	"github.com/prebid/prebid-server/errortypes"
	"github.com/prebid/prebid-server/openrtb_ext"
)

type BrightrollAdapter struct {
	URI string
}

func (a *BrightrollAdapter) MakeRequests(request *openrtb.BidRequest) ([]*adapters.RequestData, []error) {

	errs := make([]error, 0, len(request.Imp))
	if len(request.Imp) == 0 {
		err := &errortypes.BadInput{
			Message: "No impression in the bid request",
		}
		errs = append(errs, err)
		return nil, errs
	}

	validImpExists := false
	for i := 0; i < len(request.Imp); i++ {
		//Brightroll supports only banner and video impressions as of now
		if request.Imp[i].Banner != nil {
			bannerCopy := *request.Imp[i].Banner
			if bannerCopy.W == nil && bannerCopy.H == nil && len(bannerCopy.Format) > 0 {
				firstFormat := bannerCopy.Format[0]
				bannerCopy.W = &(firstFormat.W)
				bannerCopy.H = &(firstFormat.H)
			}
			request.Imp[i].Banner = &bannerCopy
			validImpExists = true
		} else if request.Imp[i].Video != nil {
			validImpExists = true
		} else {
			err := &errortypes.BadInput{
				Message: fmt.Sprintf("Brightroll only supports banner and video imps. Ignoring imp id=%s", request.Imp[i].ID),
			}
			glog.Warning("Brightroll CAPABILITY VIOLATION: only banner and video Imps supported")
			errs = append(errs, err)
			request.Imp = append(request.Imp[:i], request.Imp[i+1:]...)
			i--
		}
	}

	if !validImpExists {
		err := &errortypes.BadInput{
			Message: fmt.Sprintf("No valid impression in the bid request"),
		}
		errs = append(errs, err)
		return nil, errs
	}

	reqJSON, err := json.Marshal(request)
	if err != nil {
		errs = append(errs, err)
		return nil, errs
	}
	errors := make([]error, 0, 1)

	var bidderExt adapters.ExtImpBidder
	err = json.Unmarshal(request.Imp[0].Ext, &bidderExt)

	if err != nil {
		err = &errortypes.BadInput{
			Message: "ext.bidder not provided",
		}
		errors = append(errors, err)
		return nil, errors
	}
	var brightrollExt openrtb_ext.ExtImpBrightroll
	err = json.Unmarshal(bidderExt.Bidder, &brightrollExt)
	if err != nil {
		err = &errortypes.BadInput{
			Message: "ext.bidder.publisher not provided",
		}
		errors = append(errors, err)
		return nil, errors
	}

	if brightrollExt.Publisher == "" {
		err = &errortypes.BadInput{
			Message: "publisher is empty",
		}
		errors = append(errors, err)
		return nil, errors
	}
	thisURI := a.URI
	thisURI = thisURI + "?publisher=" + brightrollExt.Publisher
	headers := http.Header{}
	headers.Add("Content-Type", "application/json;charset=utf-8")
	headers.Add("Accept", "application/json")
	headers.Add("x-openrtb-version", "2.5")

	if request.Device != nil {
		addHeaderIfNonEmpty(headers, "User-Agent", request.Device.UA)
		addHeaderIfNonEmpty(headers, "X-Forwarded-For", request.Device.IP)
		addHeaderIfNonEmpty(headers, "Accept-Language", request.Device.Language)
		addHeaderIfNonEmpty(headers, "DNT", strconv.Itoa(int(request.Device.DNT)))
	}

	return []*adapters.RequestData{{
		Method:  "POST",
		Uri:     thisURI,
		Body:    reqJSON,
		Headers: headers,
	}}, errors
}

func (a *BrightrollAdapter) MakeBids(internalRequest *openrtb.BidRequest, externalRequest *adapters.RequestData, response *adapters.ResponseData) (*adapters.BidderResponse, []error) {

	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}

	if response.StatusCode == http.StatusBadRequest {
		return nil, []error{&errortypes.BadInput{
			Message: fmt.Sprintf("Unexpected status code: %d. ", response.StatusCode),
		}}
	}

	if response.StatusCode != http.StatusOK {
		return nil, []error{&errortypes.BadServerResponse{
			Message: fmt.Sprintf("unexpected status code: %d. Run with request.debug = 1 for more info", response.StatusCode),
		}}
	}

	var bidResp openrtb.BidResponse
	if err := json.Unmarshal(response.Body, &bidResp); err != nil {
		return nil, []error{&errortypes.BadServerResponse{
			Message: fmt.Sprintf("bad server response: %d. ", err),
		}}
	}

	bidResponse := adapters.NewBidderResponseWithBidsCapacity(len(bidResp.SeatBid[0].Bid))
	sb := bidResp.SeatBid[0]
	for i := 0; i < len(sb.Bid); i++ {
		bid := sb.Bid[i]
		bidResponse.Bids = append(bidResponse.Bids, &adapters.TypedBid{
			Bid:     &bid,
			BidType: getMediaTypeForImp(bid.ImpID, internalRequest.Imp),
		})
	}
	return bidResponse, nil
}

//Adding header fields to request header
func addHeaderIfNonEmpty(headers http.Header, headerName string, headerValue string) {
	if len(headerValue) > 0 {
		headers.Add(headerName, headerValue)
	}
}

// getMediaTypeForImp figures out which media type this bid is for.
func getMediaTypeForImp(impId string, imps []openrtb.Imp) openrtb_ext.BidType {
	mediaType := openrtb_ext.BidTypeBanner //default type
	for _, imp := range imps {
		if imp.ID == impId {
			if imp.Video != nil {
				mediaType = openrtb_ext.BidTypeVideo
			}
			return mediaType
		}
	}
	return mediaType
}

func NewBrightrollBidder(endpoint string) *BrightrollAdapter {
	return &BrightrollAdapter{
		URI: endpoint,
	}
}
