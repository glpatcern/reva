package ocdavsvc

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"time"

	rpcpb "github.com/cernbox/go-cs3apis/cs3/rpc"
	storageproviderv0alphapb "github.com/cernbox/go-cs3apis/cs3/storageprovider/v0alpha"
	"github.com/cernbox/reva/pkg/user"
	"github.com/pkg/errors"
)

func (s *svc) doPropfind(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	fn := r.URL.Path
	listChildren := r.Header.Get("Depth") != "0"

	_, status, err := readPropfind(r.Body)
	if err != nil {
		logger.Error(ctx, err)
		w.WriteHeader(status)
		return
	}

	client, err := s.getClient()
	if err != nil {
		logger.Error(ctx, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	req := &storageproviderv0alphapb.StatRequest{Filename: fn}
	res, err := client.Stat(ctx, req)
	if err != nil {
		logger.Error(ctx, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if res.Status.Code != rpcpb.Code_CODE_OK {
		if res.Status.Code == rpcpb.Code_CODE_NOT_FOUND {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		logger.Println(ctx, res.Status)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	md := res.Metadata
	mds := []*storageproviderv0alphapb.Metadata{md}
	if md.IsDir && listChildren {
		req := &storageproviderv0alphapb.ListRequest{
			Filename: fn,
		}
		stream, err := client.List(ctx, req)
		if err != nil {
			logger.Error(ctx, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				logger.Error(ctx, err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			if res.Status.Code != rpcpb.Code_CODE_OK {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}

			mds = append(mds, res.Metadata)
		}
	}

	propRes, err := s.formatPropfind(ctx, fn, mds)
	if err != nil {
		logger.Error(ctx, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("DAV", "1, 3, extended-mkcol")
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(http.StatusMultiStatus)
	w.Write([]byte(propRes))
}

// from https://github.com/golang/net/blob/e514e69ffb8bc3c76a71ae40de0118d794855992/webdav/xml.go#L178-L205
func readPropfind(r io.Reader) (pf propfindXML, status int, err error) {
	c := countingReader{r: r}
	if err = xml.NewDecoder(&c).Decode(&pf); err != nil {
		if err == io.EOF {
			if c.n == 0 {
				// An empty body means to propfind allprop.
				// http://www.webdav.org/specs/rfc4918.html#METHOD_PROPFIND
				return propfindXML{Allprop: new(struct{})}, 0, nil
			}
			err = errInvalidPropfind
		}
		return propfindXML{}, http.StatusBadRequest, err
	}

	if pf.Allprop == nil && pf.Include != nil {
		return propfindXML{}, http.StatusBadRequest, errInvalidPropfind
	}
	if pf.Allprop != nil && (pf.Prop != nil || pf.Propname != nil) {
		return propfindXML{}, http.StatusBadRequest, errInvalidPropfind
	}
	if pf.Prop != nil && pf.Propname != nil {
		return propfindXML{}, http.StatusBadRequest, errInvalidPropfind
	}
	if pf.Propname == nil && pf.Allprop == nil && pf.Prop == nil {
		return propfindXML{}, http.StatusBadRequest, errInvalidPropfind
	}
	return pf, 0, nil
}

func (s *svc) formatPropfind(ctx context.Context, fn string, mds []*storageproviderv0alphapb.Metadata) (string, error) {
	responses := []*responseXML{}
	for _, md := range mds {
		res, err := s.mdToPropResponse(ctx, md)
		if err != nil {
			return "", err
		}
		responses = append(responses, res)
	}
	responsesXML, err := xml.Marshal(&responses)
	if err != nil {
		return "", err
	}

	msg := `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" `
	msg += `xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">`
	msg += string(responsesXML) + `</d:multistatus>`
	return msg, nil
}

// TODO unused?
func (s *svc) mdsToXML(ctx context.Context, mds []*storageproviderv0alphapb.Metadata) (string, error) {
	responses := []*responseXML{}
	for _, md := range mds {
		res, err := s.mdToPropResponse(ctx, md)
		if err != nil {
			return "", err
		}
		responses = append(responses, res)
	}

	responsesXML, err := xml.Marshal(&responses)
	if err != nil {
		return "", err
	}

	msg := `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:" `
	msg += `xmlns:s="http://sabredav.org/ns" xmlns:oc="http://owncloud.org/ns">`
	msg += string(responsesXML) + `</d:multistatus>`
	return msg, nil
}

func (s *svc) newProp(key, val string) *propertyXML {
	return &propertyXML{
		XMLName:  xml.Name{Space: "", Local: key},
		Lang:     "",
		InnerXML: []byte(val),
	}
}

func (s *svc) mdToPropResponse(ctx context.Context, md *storageproviderv0alphapb.Metadata, props ...*propertyXML) (*responseXML, error) {
	propList := []*propertyXML{}
	propList = append(propList, props...)

	getETag := s.newProp("d:getetag", md.Etag)
	ocPermissions := s.newProp("oc:permissions", "WCKDNVR")
	getContentLegnth := s.newProp("d:getcontentlength", fmt.Sprintf("%d", md.Size))
	getContentType := s.newProp("d:getcontenttype", md.Mime)
	getResourceType := s.newProp("d:resourcetype", "")
	ocDownloadURL := s.newProp("oc:downloadUrl", "")
	ocDC := s.newProp("oc:dDC", "")
	propList = append(propList,
		getETag,
		ocPermissions,
		getContentLegnth,
		getContentType,
		getResourceType,
		ocDownloadURL,
		ocDC,
	)

	if md.IsDir {
		getResourceType.InnerXML = []byte("<d:collection/>")
		getContentType.InnerXML = []byte("httpd/unix-directory")
	}

	// Finder needs the the getLastModified property to work.
	t := time.Unix(int64(md.Mtime), 0).UTC()
	lasModifiedString := t.Format(time.RFC1123)
	getLastModified := s.newProp("d:getlastmodified", lasModifiedString)
	propList = append(propList, getLastModified)

	// the fileID must be xml-escaped as there are cases like public links
	// that contains a path as the file id. This path can contain &, for example,
	// which if it is not encoded properly, will result in an empty view for the user
	var fileIDEscaped bytes.Buffer
	if err := xml.EscapeText(&fileIDEscaped, []byte(md.Id)); err != nil {
		return nil, err
	}
	ocID := s.newProp("oc:id", fileIDEscaped.String())
	propList = append(propList, ocID)

	// PropStat, only HTTP/1.1 200 is sent.
	propStatList := []propstatXML{}

	propStat := propstatXML{}
	propStat.Prop = propList
	propStat.Status = "HTTP/1.1 200 OK"
	propStatList = append(propStatList, propStat)

	response := responseXML{}

	baseURI := ctx.Value("baseuri").(string)
	// the old webdav endpoint does not contain the username
	if baseURI[:18] == "/remote.php/webdav" {
		// remove username from filename
		u, ok := user.ContextGetUser(ctx)
		if !ok {
			err := errors.Wrap(contextUserRequiredErr("userrequired"), "error getting user from ctx")
			return nil, err
		}
		md.Filename = md.Filename[len(u.Username)+1:]
	}
	ref := path.Join(baseURI, md.Filename)
	if md.IsDir {
		ref += "/"
	}
	response.Href = ref

	// url encode response.Href
	encoded := &url.URL{Path: response.Href}
	response.Href = encoded.String()
	response.Propstat = propStatList
	return &response, nil
}

type countingReader struct {
	n int
	r io.Reader
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += n
	return n, err
}

// http://www.webdav.org/specs/rfc4918.html#ELEMENT_prop (for propfind)
type propfindProps []xml.Name

// http://www.webdav.org/specs/rfc4918.html#ELEMENT_propfind
type propfindXML struct {
	XMLName  xml.Name      `xml:"DAV: propfind"`
	Allprop  *struct{}     `xml:"DAV: allprop"`
	Propname *struct{}     `xml:"DAV: propname"`
	Prop     propfindProps `xml:"DAV: prop"`
	Include  propfindProps `xml:"DAV: include"`
}

type responseXML struct {
	XMLName             xml.Name      `xml:"d:response"`
	Href                string        `xml:"d:href"`
	Propstat            []propstatXML `xml:"d:propstat"`
	Status              string        `xml:"d:status,omitempty"`
	Error               *errorXML     `xml:"d:error"`
	ResponseDescription string        `xml:"d:responsedescription,omitempty"`
}

// http://www.webdav.org/specs/rfc4918.html#ELEMENT_propstat
type propstatXML struct {
	// Prop requires DAV: to be the default namespace in the enclosing
	// XML. This is due to the standard encoding/xml package currently
	// not honoring namespace declarations inside a xmltag with a
	// parent element for anonymous slice elements.
	// Use of multistatusWriter takes care of this.
	Prop                []*propertyXML `xml:"d:prop>_ignored_"`
	Status              string         `xml:"d:status"`
	Error               *errorXML      `xml:"d:error"`
	ResponseDescription string         `xml:"d:responsedescription,omitempty"`
}

// Property represents a single DAV resource property as defined in RFC 4918.
// http://www.webdav.org/specs/rfc4918.html#data.model.for.resource.properties
type propertyXML struct {
	// XMLName is the fully qualified name that identifies this property.
	XMLName xml.Name

	// Lang is an optional xml:lang attribute.
	Lang string `xml:"xml:lang,attr,omitempty"`

	// InnerXML contains the XML representation of the property value.
	// See http://www.ocwebdav.org/specs/rfc4918.html#property_values
	//
	// Property values of complex type or mixed-content must have fully
	// expanded XML namespaces or be self-contained with according
	// XML namespace declarations. They must not rely on any XML
	// namespace declarations within the scope of the XML document,
	// even including the DAV: namespace.
	InnerXML []byte `xml:",innerxml"`
}

// http://www.webdav.org/specs/rfc4918.html#ELEMENT_error
type errorXML struct {
	XMLName  xml.Name `xml:"d:error"`
	InnerXML []byte   `xml:",innerxml"`
}

var errInvalidPropfind = errors.New("webdav: invalid propfind")
